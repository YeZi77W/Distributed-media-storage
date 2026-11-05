// 有界内存、可恢复的文件下载。进度只在数据 fsync 后写入本地检查点。
#include <algorithm>
#include <errno.h>
#include <fcntl.h>
#include <openssl/sha.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>
#include "01_types.h"
#include "02_proto.h"
#include "03_util.h"
#include "01_conn.h"
#include "07_client.h"

namespace {
const size_t identity_size = APPID_SIZE + USERID_SIZE + FILEID_SIZE;
const size_t record_size = 8 + identity_size + BODYLEN_SIZE +
    FILE_DIGEST_HEX_SIZE + BODYLEN_SIZE;
const size_t offset_position = record_size - BODYLEN_SIZE;
const char magic[8] = {'T','N','V','D','L','1',0,0};

struct fd_guard {
    explicit fd_guard(int value = -1): fd(value) {}
    ~fd_guard() { if (fd != -1) close(fd); }
    int fd;
};

struct node_connections {
    explicit node_connections(std::vector<std::string> const& addresses) {
        for (size_t i = 0; i < addresses.size(); ++i)
            nodes.push_back(new conn_c(addresses[i].c_str()));
    }
    ~node_connections() {
        for (size_t i = 0; i < nodes.size(); ++i) delete nodes[i];
    }
    std::vector<conn_c*> nodes;
};

bool write_all(int fd, char const* data, size_t length) {
    size_t done = 0;
    while (done < length) {
        ssize_t count = write(fd, data + done, length - done);
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) return false;
        done += count;
    }
    return true;
}

bool read_all(int fd, char* data, size_t length) {
    size_t done = 0;
    while (done < length) {
        ssize_t count = read(fd, data + done, length - done);
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) return false;
        done += count;
    }
    return true;
}

bool sync_parent(std::string const& path) {
    size_t slash = path.find_last_of('/');
    std::string parent = slash == std::string::npos ? "." :
        slash == 0 ? "/" : path.substr(0, slash);
    fd_guard dir(open(parent.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC));
    return dir.fd != -1 && fsync(dir.fd) == 0;
}

bool hash_fd(int fd, long long size, std::string& digest) {
    struct stat info;
    if (fstat(fd, &info) || !S_ISREG(info.st_mode) || info.st_size != size)
        return false;
    SHA256_CTX context;
    if (SHA256_Init(&context) != 1) return false;
    char buffer[65536];
    for (long long offset = 0; offset < size;) {
        size_t wanted = std::min((long long)sizeof(buffer), size - offset);
        ssize_t count = pread(fd, buffer, wanted, offset);
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0 || SHA256_Update(&context, buffer, count) != 1)
            return false;
        offset += count;
    }
    unsigned char result[SHA256_DIGEST_LENGTH];
    if (SHA256_Final(result, &context) != 1) return false;
    static char const hex[] = "0123456789abcdef";
    digest.resize(SHA256_DIGEST_LENGTH * 2);
    for (size_t i = 0; i < SHA256_DIGEST_LENGTH; ++i) {
        digest[2*i] = hex[result[i] >> 4];
        digest[2*i+1] = hex[result[i] & 15];
    }
    return true;
}

bool hash_path(char const* path, long long size, std::string& digest) {
    fd_guard file(open(path, O_RDONLY | O_NOFOLLOW | O_CLOEXEC));
    return file.fd != -1 && hash_fd(file.fd, size, digest);
}

bool record_for(char const* app, char const* user, char const* file,
    long long size, std::string const& digest, long long offset,
    std::vector<char>& record) {
    if (strlen(app) >= APPID_SIZE || strlen(user) >= USERID_SIZE ||
        strlen(file) >= FILEID_SIZE || digest.size() != FILE_DIGEST_HEX_SIZE ||
        size < 0 || offset < 0 || offset > size) return false;
    record.assign(record_size, 0);
    memcpy(&record[0], magic, sizeof(magic));
    memcpy(&record[8], app, strlen(app));
    memcpy(&record[8 + APPID_SIZE], user, strlen(user));
    memcpy(&record[8 + APPID_SIZE + USERID_SIZE], file, strlen(file));
    llton(size, &record[8 + identity_size]);
    memcpy(&record[8 + identity_size + BODYLEN_SIZE],
        digest.data(), FILE_DIGEST_HEX_SIZE);
    llton(offset, &record[offset_position]);
    return true;
}

// 0=不存在，1=有效，-1=不匹配或损坏。
int read_record(std::string const& path, std::vector<char> const& expected,
    long long size, long long& offset) {
    fd_guard meta(open(path.c_str(), O_RDONLY | O_NOFOLLOW | O_CLOEXEC));
    if (meta.fd == -1) return errno == ENOENT ? 0 : -1;
    struct stat info;
    if (fstat(meta.fd, &info) || !S_ISREG(info.st_mode) ||
        info.st_size != (off_t)record_size) return -1;
    std::vector<char> data(record_size);
    if (!read_all(meta.fd, &data[0], data.size()) ||
        memcmp(&data[0], &expected[0], offset_position)) return -1;
    offset = ntoll(&data[offset_position]);
    return offset >= 0 && offset <= size ? 1 : -1;
}

bool write_record(std::string const& path, std::vector<char>& record,
    long long offset) {
    llton(offset, &record[offset_position]);
    std::string temporary = path + ".tmp";
    if (unlink(temporary.c_str()) && errno != ENOENT) return false;
    fd_guard meta(open(temporary.c_str(), O_WRONLY | O_CREAT | O_EXCL |
        O_NOFOLLOW | O_CLOEXEC, 0600));
    if (meta.fd == -1 || !write_all(meta.fd, &record[0], record.size()) ||
        fsync(meta.fd)) return false;
    if (rename(temporary.c_str(), path.c_str())) return false;
    return sync_parent(path);
}

bool write_at(int fd, char const* data, size_t length, long long offset) {
    size_t done = 0;
    while (done < length) {
        ssize_t count = pwrite(fd, data + done, length - done, offset + done);
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) return false;
        done += count;
    }
    return fsync(fd) == 0;
}
}

int client_c::filemeta(char const* appid, char const* userid,
    char const* fileid, long long* filesize, std::string& digest) {
    if (!appid || !*appid || !userid || !*userid || !fileid || !*fileid ||
        !filesize) return ERROR;
    std::string addresses;
    if (saddrs(appid, userid, fileid, addresses) != OK) return ERROR;
    std::vector<std::string> nodes;
    split(addresses.c_str(), nodes);
    if (nodes.empty()) return ERROR;
    int result = ERROR;
    for (size_t i = 0; i < nodes.size(); ++i) {
        conn_c conn(nodes[i].c_str());
        result = conn.filemeta(appid, userid, fileid, filesize, digest);
        if (result == OK) return OK;
        logger_warn("file metadata request failed on %s: %s",
            nodes[i].c_str(), conn.errdesc());
    }
    return result;
}

int client_c::download_to_file(char const* appid, char const* userid,
    char const* fileid, char const* output) {
    if (!output || !*output) return ERROR;
    long long total = 0;
    std::string digest;
    if (filemeta(appid, userid, fileid, &total, digest) != OK)
        return ERROR;
    std::string target(output), part = target + ".part";
    std::string meta = part + ".meta", lock = part + ".lock";
    fd_guard guard(open(lock.c_str(), O_RDWR | O_CREAT | O_NOFOLLOW |
        O_CLOEXEC, 0600));
    if (guard.fd == -1 || flock(guard.fd, LOCK_EX | LOCK_NB)) {
        logger_error("download target is already in use: %s", output);
        return ERROR;
    }
    struct stat existing;
    if (!lstat(target.c_str(), &existing)) {
        std::string current;
        if (hash_path(target.c_str(), total, current) && current == digest) {
            std::vector<char> prior;
            long long checkpoint = 0;
            if (record_for(appid, userid, fileid, total, digest, 0,
                prior) && read_record(meta, prior, total, checkpoint) == 1) {
                unlink(part.c_str());
                unlink(meta.c_str());
                sync_parent(target);
            }
            return OK;
        }
        logger_error("download destination already exists: %s", output);
        return ERROR;
    }
    if (errno != ENOENT) return ERROR;
    std::vector<char> record;
    if (!record_for(appid, userid, fileid, total, digest, 0, record))
        return ERROR;
    long long offset = 0;
    int state = read_record(meta, record, total, offset);
    if (state < 0) {
        logger_error("download checkpoint belongs to another file or is damaged");
        return ERROR;
    }
    fd_guard data(open(part.c_str(), O_RDWR | O_CREAT | O_NOFOLLOW |
        O_CLOEXEC, 0600));
    struct stat info;
    if (data.fd == -1 || fstat(data.fd, &info) ||
        !S_ISREG(info.st_mode) || info.st_nlink != 1) return ERROR;
    bool reset = state == 0 || info.st_size < offset;
    if (reset) offset = 0;
    if (info.st_size != offset &&
        (ftruncate(data.fd, offset) || fsync(data.fd))) return ERROR;
    if (reset) {
        if (!write_record(meta, record, offset)) return ERROR;
    }
    std::string addresses;
    if (saddrs(appid, userid, fileid, addresses) != OK) return ERROR;
    std::vector<std::string> hosts;
    split(addresses.c_str(), hosts);
    if (hosts.empty()) return ERROR;
    node_connections connections(hosts);
    size_t current_node = 0;
    while (offset < total) {
        long long wanted = std::min((long long)UPLOAD_CHUNK_MAX, total - offset);
        char* received = NULL;
        long long count = 0;
        bool got_chunk = false;
        for (size_t attempt = 0; attempt < connections.nodes.size() * 3;
            ++attempt) {
            if(observe_transfers_ && attempt) ++transfer_stats_.retry_events;
            size_t node = (current_node + attempt) % connections.nodes.size();
            int result = connections.nodes[node]->download(appid, userid,
                fileid, offset, wanted, &received, &count);
            if (result == OK && count == wanted && received) {
                if(observe_transfers_) transfer_stats_.payload["download:"+hosts[node]] += count;
                current_node = node;
                got_chunk = true;
                break;
            }
            free(received);
            received = NULL;
            if (result != SOCKET_ERROR && result != STATUS_ERROR) break;
        }
        if (!got_chunk) {
            free(received);
            logger_error("download chunk failed at offset %lld", offset);
            return ERROR;
        }
        bool good = write_at(data.fd, received, count, offset);
        free(received);
        if (!good || !write_record(meta, record, offset + count)) return ERROR;
        offset += count;
    }
    std::string current;
    if (!hash_fd(data.fd, total, current) || current != digest) {
        logger_error("download SHA-256 mismatch: %s", output);
        if (!ftruncate(data.fd, 0) && !fsync(data.fd))
            write_record(meta, record, 0);
        return ERROR;
    }
    long long latest_size = 0;
    std::string latest_digest;
    if (filemeta(appid, userid, fileid, &latest_size,
        latest_digest) != OK || latest_size != total || latest_digest != digest)
        return ERROR;
    if (fsync(data.fd)) return ERROR;
    if (link(part.c_str(), target.c_str())) {
        if (errno != EEXIST || !hash_path(target.c_str(), total, current) ||
            current != digest) return ERROR;
    }
    if (!sync_parent(target)) return ERROR;
    if (unlink(part.c_str()) && errno != ENOENT) return ERROR;
    if (unlink(meta.c_str()) && errno != ENOENT) return ERROR;
    return sync_parent(target) ? OK : ERROR;
}
