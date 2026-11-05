// 存储服务器文件发布、删除与崩溃恢复
#pragma once

#include <atomic>
#include <string>
#include <lib_acl.hpp>

std::string uploading_path(char const* filepath);
class db_c;
void cleanup_upload_sessions(db_c& db);
int sync_file_parent(char const* filepath);
int publish_file(char const* filepath, long long expected_size);
int remove_published_file(char const* filepath);
int check_file_size(char const* filepath, long long expected_size);

class recovery_c: public acl::thread {
public:
    recovery_c(void): m_stop(false) {}
    void stop(void) { m_stop.store(true); }

protected:
    void* run(void);

private:
    void recover_once(void);
    std::atomic<bool> m_stop;
};
