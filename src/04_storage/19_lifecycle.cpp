// 存储服务器文件发布、删除与崩溃恢复
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>
#include "01_types.h"
#include "01_globals.h"
#include "05_db.h"
#include "18_volume.h"
#include "19_lifecycle.h"
#include "22_hash.h"

namespace {

int sync_parent(char const* filepath) {
    std::string path(filepath);
    std::string parent = path.substr(0, path.find_last_of('/'));
    int fd = open(parent.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd == -1) {
        logger_error("open file parent fail: %s, path: %s",
            strerror(errno), parent.c_str());
        return ERROR;
    }
    int result = fsync(fd);
    int saved_errno = errno;
    if (close(fd) == -1 && result == 0) {
        result = -1;
        saved_errno = errno;
    }
    if (result == -1) {
        logger_error("sync file parent fail: %s, path: %s",
            strerror(saved_errno), parent.c_str());
        return ERROR;
    }
    return OK;
}

// 1=存在且大小匹配，0=不存在，-1=错误或大小不匹配。
int file_state(char const* path, long long expected_size) {
    struct stat info;
    if (lstat(path, &info)) {
        if (errno == ENOENT)
            return 0;
        logger_error("stat file fail: %s, path: %s", strerror(errno), path);
        return -1;
    }
    if (!S_ISREG(info.st_mode) || info.st_size != expected_size) {
        logger_error("file type or size mismatch, path: %s, expected: %lld",
            path, expected_size);
        return -1;
    }
    return 1;
}

int recover_upload(db_c& db, pending_file_t const& record) {
    if (!shared_path_ready(record.filepath.c_str()))
        return ERROR;
    std::string temporary = uploading_path(record.filepath.c_str());
    int final_state = file_state(record.filepath.c_str(), record.filesize);
    int temp_state = file_state(temporary.c_str(), record.filesize);
    if (final_state < 0 || temp_state < 0)
        return ERROR;
    if (final_state == 0 && temp_state == 0) {
        logger_warn("discard missing staged upload, fileid: %s",
            record.fileid.c_str());
        int result = db.discard_upload(record.userid.c_str(), record.fileid.c_str());
        if (result == OK) db.remove_session(record.fileid.c_str());
        return result;
    }
    if (!record.digest.empty()) {
        std::string actual;
        char const* candidate = final_state == 1 ?
            record.filepath.c_str() : temporary.c_str();
        if (!file_sha256(candidate, record.filesize, actual) ||
            actual != record.digest) {
            logger_error("pending upload digest mismatch, fileid: %s",
                record.fileid.c_str());
            return ERROR;
        }
    }
    if (final_state == 0) {
        if (publish_file(record.filepath.c_str(), record.filesize) != OK) {
            // 另一恢复线程可能已经完成重命名。
            if (file_state(record.filepath.c_str(), record.filesize) != 1)
                return ERROR;
        }
    }
    if (temp_state == 1 && unlink(temporary.c_str()) && errno != ENOENT) {
        logger_error("remove extra upload temp fail: %s", strerror(errno));
        return ERROR;
    }
    if (sync_parent(record.filepath.c_str()) != OK ||
        sync_parent(temporary.c_str()) != OK)
        return ERROR;
    int result = db.finish_upload(record.userid.c_str(), record.fileid.c_str());
    if (result == OK)
        db.set_session_status(record.fileid.c_str(),
            UPLOAD_FINALIZING, UPLOAD_DONE);
    return result;
}

int recover_delete(db_c& db, pending_file_t const& record) {
    if (remove_published_file(record.filepath.c_str()) != OK)
        return ERROR;
    return db.finish_delete(record.userid.c_str(), record.fileid.c_str());
}

} // namespace

std::string uploading_path(char const* filepath) {
    if (!filepath)
        return "";
    std::string path(filepath);
    for (std::vector<std::string>::const_iterator root = g_spaths.begin();
        root != g_spaths.end(); ++root)
        if (path.size() > root->size() &&
            path.compare(0, root->size(), *root) == 0 &&
            path[root->size()] == '/')
            return *root + "/.staging/" +
                path.substr(path.find_last_of('/') + 1) + ".uploading";
    return "";
}

int sync_file_parent(char const* filepath) {
    if (!shared_path_ready(filepath))
        return ERROR;
    return sync_parent(filepath);
}

int check_file_size(char const* filepath, long long expected_size) {
    if (!shared_path_ready(filepath))
        return ERROR;
    return file_state(filepath, expected_size) == 1 ? OK : ERROR;
}

int publish_file(char const* filepath, long long expected_size) {
    if (!shared_path_ready(filepath))
        return ERROR;
    std::string temporary = uploading_path(filepath);
    if (temporary.empty())
        return ERROR;
    if (file_state(temporary.c_str(), expected_size) != 1)
        return ERROR;
    int final_state = file_state(filepath, expected_size);
    if (final_state != 0) {
        logger_error("refuse to overwrite published file: %s", filepath);
        return ERROR;
    }
    if (rename(temporary.c_str(), filepath)) {
        logger_error("publish file rename fail: %s, path: %s",
            strerror(errno), filepath);
        return ERROR;
    }
    if (sync_parent(filepath) != OK || sync_parent(temporary.c_str()) != OK)
        return ERROR;
    return OK;
}

int remove_published_file(char const* filepath) {
    if (!shared_path_ready(filepath))
        return ERROR;
    if (unlink(filepath) && errno != ENOENT) {
        logger_error("remove file fail: %s, path: %s",
            strerror(errno), filepath);
        return ERROR;
    }
    return sync_parent(filepath);
}

void recovery_c::recover_once(void) {
    db_c db;
    if (db.connect() != OK)
        return;
    std::vector<pending_file_t> records;
    if (db.pending(records) != OK)
        return;
    for (std::vector<pending_file_t>::const_iterator record = records.begin();
        record != records.end() && !m_stop.load(); ++record) {
        int result = record->status == FILE_PUBLISHING ?
            recover_upload(db, *record) : recover_delete(db, *record);
        if (result != OK)
            logger_warn("file recovery will retry, fileid: %s, status: %d",
                record->fileid.c_str(), record->status);
        else
            logger("file recovery completed, fileid: %s, status: %d",
                record->fileid.c_str(), record->status);
    }
    cleanup_upload_sessions(db);
}

void* recovery_c::run(void) {
    while (!m_stop.load()) {
        recover_once();
        for (int i = 0; i < 30 && !m_stop.load(); ++i)
            sleep(1);
    }
    return NULL;
}
