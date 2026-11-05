// 存储服务器文件元数据与生命周期
#pragma once

#include <string>
#include <vector>
#include <mysql.h>

enum file_status_t {
    FILE_READY = 0,
    FILE_PUBLISHING = 1,
    FILE_DELETING = 2
};

struct pending_file_t {
    std::string fileid;
    std::string userid;
    std::string filepath;
    std::string digest;
    long long filesize;
    file_status_t status;
};

enum upload_status_t {
    UPLOAD_ACTIVE = 0,
    UPLOAD_FINALIZING = 1,
    UPLOAD_DONE = 2
};

struct upload_session_t {
    std::string appid;
    std::string userid;
    std::string fileid;
    std::string filepath;
    std::string digest; // SHA-256 hex
    long long filesize;
    long long offset;
    bool expired;
    upload_status_t status;
};

class db_c {
public:
    db_c(void);
    ~db_c(void);
    int connect(void);

    // 查询只返回已经发布的文件，数据库是状态的唯一权威来源。
    int get(char const* appid, char const* userid, char const* fileid,
        std::string& filepath, long long* filesize) const;
    int get_digest(char const* appid, char const* userid,
        char const* fileid, std::string& digest) const;
    int set_digest_if_missing(char const* appid, char const* userid,
        char const* fileid, char const* filepath, long long filesize,
        char const* digest) const;

    // 上传：完整临时文件落盘后登记待发布，发布成功再转为可读。
    int stage_upload(char const* appid, char const* userid,
        char const* fileid, char const* filepath, long long filesize,
        char const* digest) const;
    int finish_upload(char const* userid, char const* fileid) const;
    int discard_upload(char const* userid, char const* fileid) const;

    // 删除：先标记为不可读，再删除文件，最后删除元数据。
    int mark_deleting(char const* appid, char const* userid,
        char const* fileid, std::string& filepath,
        long long* filesize) const;
    int finish_delete(char const* userid, char const* fileid) const;

    // 扫描超过宽限期的中间状态，供后台恢复线程重试。
    int pending(std::vector<pending_file_t>& files) const;

    // 断点续传会话，按 fileid 唯一；跨节点用 InnoDB 行锁串行化分块。
    int create_session(upload_session_t const& session) const;
    int get_session(char const* appid, char const* userid,
        char const* fileid, upload_session_t& session,
        bool for_update = false) const;
    int begin(void) const;
    int commit(void) const;
    void rollback(void) const;
    int advance_session(char const* fileid, long long old_offset,
        long long new_offset) const;
    int set_session_status(char const* fileid, upload_status_t from,
        upload_status_t to) const;
    int remove_session(char const* fileid) const;
    int expired_sessions(std::vector<upload_session_t>& sessions) const;

private:
    bool escape(char const* value, std::string& escaped) const;
    int change_status(char const* userid, char const* fileid,
        file_status_t from, file_status_t to) const;
    int remove_status(char const* userid, char const* fileid,
        file_status_t expected) const;
    std::string table_of_user(char const* userid) const;
    unsigned int hash(char const* buf, size_t len) const;

    MYSQL* m_mysql;
};
