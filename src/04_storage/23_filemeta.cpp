#include <string.h>
#include "01_types.h"
#include "02_proto.h"
#include "03_util.h"
#include "05_db.h"
#include "11_service.h"
#include "18_volume.h"
#include "19_lifecycle.h"
#include "22_hash.h"

namespace {
bool digest_valid(std::string const& digest) {
    if (digest.size() != FILE_DIGEST_HEX_SIZE) return false;
    for (size_t i = 0; i < digest.size(); ++i)
        if (!((digest[i] >= '0' && digest[i] <= '9') ||
              (digest[i] >= 'a' && digest[i] <= 'f'))) return false;
    return true;
}
}

bool service_c::filemeta(acl::socket_stream* conn, long long bodylen) const {
    const size_t identity = APPID_SIZE + USERID_SIZE + FILEID_SIZE;
    if (bodylen != (long long)identity) {
        error(conn, -1, "invalid file metadata request length");
        return false;
    }
    char body[identity];
    size_t done = 0;
    while (done < sizeof(body)) {
        long long count = conn->read(body + done, sizeof(body) - done);
        if (count <= 0) return false;
        done += count;
    }
    char const* app = body;
    char const* user = app + APPID_SIZE;
    char const* file = user + USERID_SIZE;
    if (!memchr(app, 0, APPID_SIZE) || !memchr(user, 0, USERID_SIZE) ||
        !memchr(file, 0, FILEID_SIZE) || !*app || !*user || !*file ||
        strlen(user) > 128)
        return error(conn, -1, "invalid file metadata identity");

    db_c db;
    std::string path, digest;
    long long size = 0;
    if (db.connect() != OK || db.get(app, user, file, path, &size) != OK ||
        !shared_path_ready(path.c_str()) ||
        check_file_size(path.c_str(), size) != OK ||
        db.get_digest(app, user, file, digest) != OK)
        return error(conn, -1, "ready file metadata unavailable");
    if (digest.empty()) {
        std::string computed;
        if (!file_sha256(path.c_str(), size, computed))
            return error(conn, -1, "calculate file digest fail");
        if (db.set_digest_if_missing(app, user, file, path.c_str(),
            size, computed.c_str()) != OK) {
            // 另一节点可能已经写入相同的摘要。
            if (db.get_digest(app, user, file, digest) != OK ||
                digest != computed)
                return error(conn, -1, "file metadata changed during hashing");
        } else digest = computed;
    }
    if (!digest_valid(digest))
        return error(conn, -1, "stored file digest is invalid");
    char reply[HEADLEN + BODYLEN_SIZE + FILE_DIGEST_HEX_SIZE] = {};
    llton(BODYLEN_SIZE + FILE_DIGEST_HEX_SIZE, reply);
    reply[BODYLEN_SIZE] = CMD_STORAGE_REPLY;
    llton(size, reply + HEADLEN);
    memcpy(reply + HEADLEN + BODYLEN_SIZE, digest.data(), digest.size());
    size_t sent = 0;
    while (sent < sizeof(reply)) {
        long long count = conn->write(reply + sent, sizeof(reply) - sent);
        if (count <= 0) return false;
        sent += count;
    }
    return true;
}
