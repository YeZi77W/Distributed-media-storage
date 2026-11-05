// 顺序分块断点续传。只有 COMMIT 后才接入文件发布状态机。
#include <algorithm>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <openssl/sha.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>
#include "01_types.h"
#include "02_proto.h"
#include "03_util.h"
#include "01_globals.h"
#include "05_db.h"
#include "11_service.h"
#include "18_volume.h"
#include "19_lifecycle.h"
#include "22_hash.h"

namespace {
const size_t identity_size = APPID_SIZE + USERID_SIZE + FILEID_SIZE;

bool receive(acl::socket_stream* conn, char* data, size_t size) {
    size_t done = 0;
    while (done < size) {
        long long count = conn->read(data + done, size - done);
        if (count <= 0) return false;
        done += count;
    }
    return true;
}

bool transmit(acl::socket_stream* conn, char const* data, size_t size) {
    size_t done = 0;
    while (done < size) {
        long long count = conn->write(data + done, size - done);
        if (count <= 0) return false;
        done += count;
    }
    return true;
}

bool identity(char const* data, char* app, char* user, char* file) {
    char const* u = data + APPID_SIZE;
    char const* f = u + USERID_SIZE;
    if (!memchr(data, 0, APPID_SIZE) || !memchr(u, 0, USERID_SIZE) ||
        !memchr(f, 0, FILEID_SIZE) || !*data || !*u || !*f ||
        strlen(u) > 128) return false;
    memcpy(app, data, APPID_SIZE);
    memcpy(user, u, USERID_SIZE);
    memcpy(file, f, FILEID_SIZE);
    return true;
}

std::string hex_digest(unsigned char const* digest) {
    static char const digits[] = "0123456789abcdef";
    std::string result(UPLOAD_DIGEST_SIZE * 2, '0');
    for (size_t i = 0; i < UPLOAD_DIGEST_SIZE; ++i) {
        result[2*i] = digits[digest[i] >> 4];
        result[2*i+1] = digits[digest[i] & 15];
    }
    return result;
}

bool write_chunk(char const* path, long long offset,
    char const* data, size_t length) {
    int fd = open(path, O_RDWR | O_CREAT | O_NOFOLLOW | O_CLOEXEC, 0644);
    if (fd == -1) return false;
    struct stat info;
    bool good = !fstat(fd, &info) && S_ISREG(info.st_mode) &&
        info.st_size >= offset;
    // 上次崩溃时可能已写盘，却没有提交数据库进度。
    if (good && info.st_size != offset)
        good = !ftruncate(fd, offset) && !fsync(fd);
    size_t done = 0;
    while (good && done < length) {
        ssize_t count = pwrite(fd, data + done, length - done, offset + done);
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) good = false;
        else done += count;
    }
    if (good) good = !fsync(fd);
    if (close(fd)) good = false;
    if (good) good = sync_file_parent(path) == OK;
    return good;
}

bool empty_file(char const* path) {
    int fd = open(path, O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC,
        0644);
    if (fd == -1 && errno == EEXIST) return true;
    if (fd == -1) return false;
    bool good = !fsync(fd);
    if (close(fd)) good = false;
    return good && sync_file_parent(path) == OK;
}
}

bool service_c::upload_progress(acl::socket_stream* conn,
    long long offset, char status) const {
    char reply[HEADLEN + BODYLEN_SIZE + 1] = {};
    llton(BODYLEN_SIZE + 1, reply);
    reply[BODYLEN_SIZE] = CMD_STORAGE_REPLY;
    llton(offset, reply + HEADLEN);
    reply[HEADLEN + BODYLEN_SIZE] = status;
    return transmit(conn, reply, sizeof(reply));
}

bool service_c::upload_init(acl::socket_stream* conn,
    long long bodylen) const {
    if (bodylen != (long long)identity_size + BODYLEN_SIZE +
        UPLOAD_DIGEST_SIZE) {
        error(conn, -1, "invalid upload init length");
        return false;
    }
    char body[identity_size + BODYLEN_SIZE + UPLOAD_DIGEST_SIZE];
    if (!receive(conn, body, sizeof(body))) return false;
    char app[APPID_SIZE], user[USERID_SIZE], file[FILEID_SIZE];
    if (!identity(body, app, user, file))
        return error(conn, -1, "invalid upload identity");
    long long size = ntoll(body + identity_size);
    if (size < 0) return error(conn, -1, "invalid upload file size");
    std::string digest = hex_digest(
        reinterpret_cast<unsigned char const*>(body + identity_size + BODYLEN_SIZE));
    db_c db;
    if (db.connect() != OK) return error(conn, -1, "database unavailable");
    upload_session_t session;
    if (db.get_session(app, user, file, session) != OK) {
        std::string published;
        long long published_size = 0;
        if (db.get(app, user, file, published, &published_size) == OK)
            return error(conn, -1, "fileid is already published");
        char path[PATH_MAX + 1];
        if (genpath(path) != OK) return error(conn, -1, "create upload path fail");
        session.appid = app;
        session.userid = user;
        session.fileid = file;
        session.filepath = path;
        session.filesize = size;
        session.offset = 0;
        session.digest = digest;
        session.status = UPLOAD_ACTIVE;
        if (db.create_session(session) != OK &&
            db.get_session(app, user, file, session) != OK)
            return error(conn, -1, "create upload session fail");
    }
    if (session.filesize != size || session.digest != digest)
        return error(conn, -1, "fileid belongs to a different upload");
    if (session.status == UPLOAD_DONE) {
        std::string published;
        long long published_size = 0;
        if (db.get(app, user, file, published, &published_size) != OK ||
            published_size != size ||
            check_file_size(published.c_str(), size) != OK)
            return error(conn, -1, "completed upload is no longer available");
    }
    if (!shared_path_ready(session.filepath.c_str()))
        return error(conn, -1, "shared volume unavailable");
    return upload_progress(conn, session.offset, session.status);
}

bool service_c::upload_status(acl::socket_stream* conn,
    long long bodylen) const {
    if (bodylen != (long long)identity_size) {
        error(conn, -1, "invalid upload status length");
        return false;
    }
    char body[identity_size];
    if (!receive(conn, body, sizeof(body))) return false;
    char app[APPID_SIZE], user[USERID_SIZE], file[FILEID_SIZE];
    if (!identity(body, app, user, file))
        return error(conn, -1, "invalid upload identity");
    db_c db;
    upload_session_t session;
    if (db.connect() != OK ||
        db.get_session(app, user, file, session) != OK)
        return error(conn, -1, "upload session not found");
    if (session.status == UPLOAD_DONE) {
        std::string published;
        long long published_size = 0;
        if (db.get(app, user, file, published, &published_size) != OK ||
            published_size != session.filesize ||
            check_file_size(published.c_str(), published_size) != OK)
            return error(conn, -1, "completed upload is no longer available");
    }
    return upload_progress(conn, session.offset, session.status);
}

bool service_c::upload_chunk(acl::socket_stream* conn,
    long long bodylen) const {
    const long long fixed = identity_size + BODYLEN_SIZE * 2;
    if (bodylen <= fixed || bodylen > fixed + UPLOAD_CHUNK_MAX) {
        error(conn, -1, "invalid upload chunk length");
        return false;
    }
    char fields[identity_size + BODYLEN_SIZE * 2];
    if (!receive(conn, fields, sizeof(fields))) return false;
    char app[APPID_SIZE], user[USERID_SIZE], file[FILEID_SIZE];
    if (!identity(fields, app, user, file)) {
        error(conn, -1, "invalid upload identity");
        return false;
    }
    long long offset = ntoll(fields + identity_size);
    long long length = ntoll(fields + identity_size + BODYLEN_SIZE);
    if (offset < 0 || length != bodylen - fixed || length <= 0 ||
        length > UPLOAD_CHUNK_MAX) {
        error(conn, -1, "invalid upload chunk offset or size");
        return false;
    }
    std::vector<char> data(length);
    if (!receive(conn, &data[0], length)) return false;
    db_c db;
    if (db.connect() != OK || db.begin() != OK)
        return error(conn, -1, "database unavailable");
    upload_session_t session;
    if (db.get_session(app, user, file, session, true) != OK ||
        session.status != UPLOAD_ACTIVE || offset != session.offset ||
        length > session.filesize - offset ||
        !shared_path_ready(session.filepath.c_str())) {
        db.rollback();
        return error(conn, -1, "upload session offset or state changed");
    }
    std::string path = uploading_path(session.filepath.c_str());
    if (path.empty() || !write_chunk(path.c_str(), offset, &data[0], length) ||
        db.advance_session(file, offset, offset + length) != OK ||
        db.commit() != OK) {
        db.rollback();
        return error(conn, -1, "upload chunk was not confirmed; query status");
    }
    return upload_progress(conn, offset + length, UPLOAD_ACTIVE);
}

bool service_c::upload_commit(acl::socket_stream* conn,
    long long bodylen) const {
    if (bodylen != (long long)identity_size) {
        error(conn, -1, "invalid upload commit length");
        return false;
    }
    char body[identity_size];
    if (!receive(conn, body, sizeof(body))) return false;
    char app[APPID_SIZE], user[USERID_SIZE], file[FILEID_SIZE];
    if (!identity(body, app, user, file))
        return error(conn, -1, "invalid upload identity");
    db_c db;
    if (db.connect() != OK || db.begin() != OK)
        return error(conn, -1, "database unavailable");
    upload_session_t session;
    if (db.get_session(app, user, file, session, true) != OK ||
        session.offset != session.filesize ||
        !shared_path_ready(session.filepath.c_str())) {
        db.rollback();
        return error(conn, -1, "upload is incomplete or unavailable");
    }
    std::string path = uploading_path(session.filepath.c_str());
    if (session.status == UPLOAD_ACTIVE) {
        if (session.filesize == 0 && !empty_file(path.c_str())) {
            db.rollback();
            return error(conn, -1, "create empty upload fail");
        }
        std::string digest;
        if (!file_sha256(path.c_str(), session.filesize, digest) ||
            digest != session.digest ||
            db.stage_upload(app, user, file, session.filepath.c_str(),
                session.filesize, digest.c_str()) != OK ||
            db.set_session_status(file, UPLOAD_ACTIVE, UPLOAD_FINALIZING) != OK ||
            db.commit() != OK) {
            db.rollback();
            return error(conn, -1, "upload checksum or publish preparation fail");
        }
    } else db.rollback();
    if (session.status == UPLOAD_DONE) {
        std::string published;
        long long published_size = 0;
        if (db.get(app, user, file, published, &published_size) != OK ||
            published_size != session.filesize ||
            check_file_size(published.c_str(), published_size) != OK)
            return error(conn, -1, "completed upload is no longer available");
    }
    if (session.status != UPLOAD_DONE) {
        if (check_file_size(session.filepath.c_str(), session.filesize) != OK &&
            publish_file(session.filepath.c_str(), session.filesize) != OK &&
            check_file_size(session.filepath.c_str(), session.filesize) != OK)
            return error(conn, -1, "upload publication pending; retry commit");
        if (db.finish_upload(user, file) != OK)
            return error(conn, -1, "upload publication pending; retry commit");
        if (db.set_session_status(file, UPLOAD_FINALIZING, UPLOAD_DONE) != OK)
            return error(conn, -1, "upload complete; retry commit");
    }
    return upload_progress(conn, session.filesize, UPLOAD_DONE);
}

void cleanup_upload_sessions(db_c& db) {
    std::vector<upload_session_t> candidates;
    if (db.expired_sessions(candidates) != OK) return;
    for (size_t i = 0; i < candidates.size(); ++i) {
        upload_session_t session;
        if (db.begin() != OK) return;
        if (db.get_session(candidates[i].appid.c_str(),
            candidates[i].userid.c_str(), candidates[i].fileid.c_str(),
            session, true) != OK || !session.expired ||
            session.status == UPLOAD_FINALIZING) {
            db.rollback();
            continue;
        }
        if (session.status == UPLOAD_ACTIVE) {
            if (!shared_path_ready(session.filepath.c_str())) {
                db.rollback();
                continue;
            }
            std::string path = uploading_path(session.filepath.c_str());
            if (path.empty() || (unlink(path.c_str()) && errno != ENOENT) ||
                sync_file_parent(path.c_str()) != OK) {
                db.rollback();
                continue;
            }
        }
        if (db.remove_session(session.fileid.c_str()) != OK ||
            db.commit() != OK) db.rollback();
    }
}
