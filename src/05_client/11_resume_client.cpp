// 文件路径上传默认使用可恢复的分块协议。
#include <algorithm>
#include <errno.h>
#include <fcntl.h>
#include <openssl/sha.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>
#include "01_types.h"
#include "02_proto.h"
#include "03_util.h"
#include "01_conn.h"
#include "07_client.h"

namespace {
bool read_at(int fd, char* data, size_t size, off_t offset) {
    size_t done = 0;
    while (done < size) {
        ssize_t count = pread(fd, data + done, size - done, offset + done);
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) return false;
        done += count;
    }
    return true;
}

bool fingerprint(int fd, long long size,
    unsigned char digest[UPLOAD_DIGEST_SIZE]) {
    SHA256_CTX ctx;
    if (SHA256_Init(&ctx) != 1) return false;
    char buffer[65536];
    for (long long offset = 0; offset < size;) {
        size_t count = std::min((long long)sizeof(buffer), size - offset);
        if (!read_at(fd, buffer, count, offset) ||
            SHA256_Update(&ctx, buffer, count) != 1) return false;
        offset += count;
    }
    return SHA256_Final(digest, &ctx) == 1;
}
}

int client_c::upload_resumable(char const* appid, char const* userid,
    char const* fileid, char const* filepath) {
    if (!appid || !*appid || !userid || !*userid || !fileid || !*fileid ||
        !filepath || !*filepath) return ERROR;
    int fd = open(filepath, O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
    if (fd == -1) {
        logger_error("open upload file fail: %s", strerror(errno));
        return ERROR;
    }
    struct stat info;
    if (fstat(fd, &info) || !S_ISREG(info.st_mode) || info.st_size < 0) {
        close(fd);
        return ERROR;
    }
    long long size = info.st_size;
    unsigned char digest[UPLOAD_DIGEST_SIZE];
    if (!fingerprint(fd, size, digest)) {
        close(fd);
        return ERROR;
    }
    std::string addresses;
    if (saddrs(appid, userid, fileid, addresses) != OK) {
        close(fd);
        return ERROR;
    }
    std::vector<std::string> nodes;
    split(addresses.c_str(), nodes);
    if (nodes.empty()) {
        close(fd);
        return ERROR;
    }
    std::vector<char> chunk(UPLOAD_CHUNK_MAX);
    int result = ERROR;
    for (size_t attempt = 0; attempt < nodes.size() * 10; ++attempt) {
        if(observe_transfers_ && attempt) ++transfer_stats_.retry_events;
        conn_c conn(nodes[attempt % nodes.size()].c_str());
        long long offset = 0;
        char status = 0;
        result = conn.upload_init(appid, userid, fileid, size, digest,
            &offset, &status);
        if (result == SOCKET_ERROR) continue;
        if (result != OK) {
            logger_error("resume init fail: %s", conn.errdesc());
            break;
        }
        if (offset < 0 || offset > size || status < 0 || status > 2) {
            result = ERROR;
            break;
        }
        bool stale_offset = false;
        while (status == 0 && offset < size) {
            size_t count = std::min((long long)chunk.size(), size - offset);
            if (!read_at(fd, &chunk[0], count, offset)) {
                result = ERROR;
                break;
            }
            long long confirmed = 0;
            result = conn.upload_chunk(appid, userid, fileid, offset,
                &chunk[0], count, &confirmed);
            if (result != OK) {
                stale_offset = result == STATUS_ERROR;
                break;
            }
            if (confirmed != offset + count) {
                result = ERROR;
                break;
            }
            if(observe_transfers_) transfer_stats_.payload["upload:"+nodes[attempt % nodes.size()]] += count;
            offset = confirmed;
        }
        if (result == OK && status != 2) {
            result = conn.upload_commit(appid, userid, fileid,
                &offset, &status);
            if (result == OK && (status != 2 || offset != size))
                result = ERROR;
        }
        if (result == OK) break;
        if (result == SOCKET_ERROR || stale_offset || status == 1) {
            usleep(200000);
            continue;
        }
        logger_error("resume upload fail: %s", conn.errdesc());
        break;
    }
    close(fd);
    return result;
}
