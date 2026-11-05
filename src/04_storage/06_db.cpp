// 存储服务器文件元数据与生命周期
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "01_types.h"
#include "01_globals.h"
#include "05_db.h"

db_c::db_c(void): m_mysql(mysql_init(NULL)) {
    if (!m_mysql)
        logger_error("create storage dao fail: mysql_init returned null");
}

db_c::~db_c(void) {
    if (m_mysql)
        mysql_close(m_mysql);
}

int db_c::connect(void) {
    if (!m_mysql)
        return ERROR;
    unsigned int timeout = cfg_mtimeout > 0 ? cfg_mtimeout : 30;
    if (mysql_options(m_mysql, MYSQL_OPT_CONNECT_TIMEOUT, &timeout) ||
        mysql_options(m_mysql, MYSQL_OPT_READ_TIMEOUT, &timeout) ||
        mysql_options(m_mysql, MYSQL_OPT_WRITE_TIMEOUT, &timeout)) {
        logger_error("set storage database timeout fail: %s",
            mysql_error(m_mysql));
        return ERROR;
    }
    MYSQL* mysql = m_mysql;
    for (std::vector<std::string>::const_iterator address = g_maddrs.begin();
        address != g_maddrs.end(); ++address)
        if ((m_mysql = mysql_real_connect(mysql, address->c_str(),
            cfg_dbuser, cfg_dbpass, cfg_dbname, 0, NULL, 0)))
            return OK;
    m_mysql = mysql;
    logger_error("connect storage database fail: %s", mysql_error(m_mysql));
    return ERROR;
}

bool db_c::escape(char const* value, std::string& escaped) const {
    if (!m_mysql || !value)
        return false;
    size_t length = strlen(value);
    std::vector<char> buffer(length * 2 + 1);
    unsigned long written = mysql_real_escape_string(m_mysql,
        &buffer[0], value, length);
    if (written == (unsigned long)-1) {
        logger_error("escape storage SQL value fail: %s", mysql_error(m_mysql));
        return false;
    }
    escaped.assign(&buffer[0], written);
    return true;
}

int db_c::get(char const* appid, char const* userid, char const* fileid,
    std::string& filepath, long long* filesize) const {
    if (!m_mysql || !appid || !userid || !fileid || !filesize)
        return ERROR;
    std::string app, user, file;
    if (!escape(appid, app) || !escape(userid, user) ||
        !escape(fileid, file))
        return ERROR;

    std::string table = table_of_user(userid);
    acl::string sql;
    sql.format("SELECT file_path,file_size FROM %s WHERE id='%s' "
        "AND appid='%s' AND userid='%s' AND status=%d;",
        table.c_str(), file.c_str(), app.c_str(), user.c_str(), FILE_READY);
    if (mysql_query(m_mysql, sql.c_str())) {
        logger_error("query ready file fail: %s", mysql_error(m_mysql));
        return ERROR;
    }
    MYSQL_RES* result = mysql_store_result(m_mysql);
    if (!result) {
        logger_error("read ready file result fail: %s", mysql_error(m_mysql));
        return ERROR;
    }
    MYSQL_ROW row = mysql_fetch_row(result);
    if (!row || !row[0] || !row[1]) {
        mysql_free_result(result);
        return ERROR;
    }
    errno = 0;
    char* end = NULL;
    long long size = strtoll(row[1], &end, 10);
    if (errno == ERANGE || end == row[1] || *end || size < 0) {
        logger_error("invalid stored file size, fileid: %s", fileid);
        mysql_free_result(result);
        return ERROR;
    }
    filepath = row[0];
    *filesize = size;
    mysql_free_result(result);
    return OK;
}

int db_c::get_digest(char const* appid, char const* userid,
    char const* fileid, std::string& digest) const {
    std::string app, user, file;
    if (!escape(appid, app) || !escape(userid, user) ||
        !escape(fileid, file)) return ERROR;
    acl::string sql;
    sql.format("SELECT sha256 FROM %s WHERE id='%s' AND appid='%s' "
        "AND userid='%s' AND status=%d;", table_of_user(userid).c_str(),
        file.c_str(), app.c_str(), user.c_str(), FILE_READY);
    if (mysql_query(m_mysql, sql.c_str())) return ERROR;
    MYSQL_RES* result = mysql_store_result(m_mysql);
    if (!result) return ERROR;
    MYSQL_ROW row = mysql_fetch_row(result);
    if (!row) {
        mysql_free_result(result);
        return ERROR;
    }
    digest = row[0] ? row[0] : "";
    mysql_free_result(result);
    return OK;
}

int db_c::set_digest_if_missing(char const* appid, char const* userid,
    char const* fileid, char const* filepath, long long filesize,
    char const* digest) const {
    if (!digest || strlen(digest) != 64 || filesize < 0) return ERROR;
    std::string app, user, file, path, hash;
    if (!escape(appid, app) || !escape(userid, user) ||
        !escape(fileid, file) || !escape(filepath, path) ||
        !escape(digest, hash)) return ERROR;
    acl::string sql;
    sql.format("UPDATE %s SET sha256='%s' WHERE id='%s' AND appid='%s' "
        "AND userid='%s' AND status=%d AND file_path='%s' "
        "AND file_size=%lld AND sha256 IS NULL;",
        table_of_user(userid).c_str(), hash.c_str(), file.c_str(),
        app.c_str(), user.c_str(), FILE_READY, path.c_str(), filesize);
    if (mysql_query(m_mysql, sql.c_str())) return ERROR;
    return mysql_affected_rows(m_mysql) == 1 ? OK : ERROR;
}

int db_c::stage_upload(char const* appid, char const* userid,
    char const* fileid, char const* filepath, long long filesize,
    char const* digest) const {
    if (!m_mysql || !appid || !userid || !fileid || !filepath ||
        !digest || strlen(digest) != 64 || filesize < 0)
        return ERROR;
    std::string app, user, file, path, hash;
    if (!escape(appid, app) || !escape(userid, user) ||
        !escape(fileid, file) || !escape(filepath, path) ||
        !escape(digest, hash))
        return ERROR;
    acl::string sql;
    sql.format("INSERT INTO %s (id,appid,userid,status,file_path,file_size,sha256) "
        "VALUES ('%s','%s','%s',%d,'%s',%lld,'%s');",
        table_of_user(userid).c_str(), file.c_str(), app.c_str(),
        user.c_str(), FILE_PUBLISHING, path.c_str(), filesize, hash.c_str());
    if (mysql_query(m_mysql, sql.c_str())) {
        logger_error("stage upload fail: %s, fileid: %s",
            mysql_error(m_mysql), fileid);
        return ERROR;
    }
    return OK;
}

int db_c::change_status(char const* userid, char const* fileid,
    file_status_t from, file_status_t to) const {
    if (!m_mysql || !userid || !fileid)
        return ERROR;
    std::string file;
    if (!escape(fileid, file))
        return ERROR;
    acl::string sql;
    sql.format("UPDATE %s SET status=%d WHERE id='%s' AND status=%d;",
        table_of_user(userid).c_str(), to, file.c_str(), from);
    if (mysql_query(m_mysql, sql.c_str())) {
        logger_error("file state change fail: %s, fileid: %s, %d -> %d",
            mysql_error(m_mysql), fileid, from, to);
        return ERROR;
    }
    if (mysql_affected_rows(m_mysql) == 1)
        return OK;
    // 另一实例可能已经完成同一状态转换。
    sql.format("SELECT status FROM %s WHERE id='%s';",
        table_of_user(userid).c_str(), file.c_str());
    if (mysql_query(m_mysql, sql.c_str()))
        return ERROR;
    MYSQL_RES* result = mysql_store_result(m_mysql);
    if (!result)
        return ERROR;
    MYSQL_ROW row = mysql_fetch_row(result);
    bool already_done = row && row[0] && atoi(row[0]) == to;
    mysql_free_result(result);
    return already_done ? OK : ERROR;
}

int db_c::remove_status(char const* userid, char const* fileid,
    file_status_t expected) const {
    if (!m_mysql || !userid || !fileid)
        return ERROR;
    std::string file;
    if (!escape(fileid, file))
        return ERROR;
    acl::string sql;
    sql.format("DELETE FROM %s WHERE id='%s' AND status=%d;",
        table_of_user(userid).c_str(), file.c_str(), expected);
    if (mysql_query(m_mysql, sql.c_str())) {
        logger_error("remove file metadata fail: %s, fileid: %s, status: %d",
            mysql_error(m_mysql), fileid, expected);
        return ERROR;
    }
    if (mysql_affected_rows(m_mysql) == 1)
        return OK;
    sql.format("SELECT id FROM %s WHERE id='%s';",
        table_of_user(userid).c_str(), file.c_str());
    if (mysql_query(m_mysql, sql.c_str()))
        return ERROR;
    MYSQL_RES* result = mysql_store_result(m_mysql);
    if (!result)
        return ERROR;
    MYSQL_ROW row = mysql_fetch_row(result);
    bool already_removed = !row && !mysql_errno(m_mysql);
    mysql_free_result(result);
    return already_removed ? OK : ERROR;
}

int db_c::finish_upload(char const* userid, char const* fileid) const {
    return change_status(userid, fileid, FILE_PUBLISHING, FILE_READY);
}

int db_c::discard_upload(char const* userid, char const* fileid) const {
    return remove_status(userid, fileid, FILE_PUBLISHING);
}

int db_c::mark_deleting(char const* appid, char const* userid,
    char const* fileid, std::string& filepath, long long* filesize) const {
    if (get(appid, userid, fileid, filepath, filesize) == OK)
        return change_status(userid, fileid, FILE_READY, FILE_DELETING);
    // 客户端重试时，可能已被另一实例标记为待删除，继续幂等清理。
    if (!m_mysql || !appid || !userid || !fileid || !filesize)
        return ERROR;
    std::string app, user, file;
    if (!escape(appid, app) || !escape(userid, user) ||
        !escape(fileid, file))
        return ERROR;
    acl::string sql;
    sql.format("SELECT file_path,file_size FROM %s WHERE id='%s' "
        "AND appid='%s' AND userid='%s' AND status=%d;",
        table_of_user(userid).c_str(), file.c_str(), app.c_str(),
        user.c_str(), FILE_DELETING);
    if (mysql_query(m_mysql, sql.c_str()))
        return ERROR;
    MYSQL_RES* result = mysql_store_result(m_mysql);
    if (!result)
        return ERROR;
    MYSQL_ROW row = mysql_fetch_row(result);
    if (!row || !row[0] || !row[1]) {
        mysql_free_result(result);
        return ERROR;
    }
    errno = 0;
    char* end = NULL;
    long long size = strtoll(row[1], &end, 10);
    if (errno == ERANGE || end == row[1] || *end || size < 0) {
        mysql_free_result(result);
        return ERROR;
    }
    filepath = row[0];
    *filesize = size;
    mysql_free_result(result);
    return OK;
}

int db_c::finish_delete(char const* userid, char const* fileid) const {
    return remove_status(userid, fileid, FILE_DELETING);
}

int db_c::pending(std::vector<pending_file_t>& files) const {
    if (!m_mysql)
        return ERROR;
    for (int shard = 1; shard <= 3; ++shard) {
        acl::string sql;
        sql.format("SELECT id,userid,file_path,file_size,status,sha256 FROM "
            "t_file_%02d WHERE status IN (%d,%d) "
            "AND update_time < NOW() - INTERVAL 120 SECOND "
            "ORDER BY update_time;",
            shard, FILE_PUBLISHING, FILE_DELETING);
        if (mysql_query(m_mysql, sql.c_str())) {
            logger_error("scan pending file records fail: %s",
                mysql_error(m_mysql));
            return ERROR;
        }
        MYSQL_RES* result = mysql_store_result(m_mysql);
        if (!result) {
            logger_error("read pending file records fail: %s",
                mysql_error(m_mysql));
            return ERROR;
        }
        MYSQL_ROW row;
        while ((row = mysql_fetch_row(result))) {
            if (!row[0] || !row[1] || !row[2] || !row[3] || !row[4]) {
                logger_error("pending file record has null columns");
                continue;
            }
            errno = 0;
            char* end = NULL;
            long long size = strtoll(row[3], &end, 10);
            int status = atoi(row[4]);
            if (errno == ERANGE || end == row[3] || *end || size < 0 ||
                (status != FILE_PUBLISHING && status != FILE_DELETING)) {
                logger_error("pending file record has invalid fields: %s", row[0]);
                continue;
            }
            pending_file_t file = {row[0], row[1], row[2],
                row[5] ? row[5] : "", size,
                static_cast<file_status_t>(status)};
            files.push_back(file);
        }
        if (mysql_errno(m_mysql)) {
            logger_error("fetch pending file records fail: %s",
                mysql_error(m_mysql));
            mysql_free_result(result);
            return ERROR;
        }
        mysql_free_result(result);
    }
    return OK;
}

std::string db_c::table_of_user(char const* userid) const {
    char table[16];
    sprintf(table, "t_file_%02d",
        (hash(userid, strlen(userid)) & 0x7FFFFFFF) % 3 + 1);
    return table;
}

unsigned int db_c::hash(char const* buf, size_t len) const {
    unsigned int h = 0;
    for (size_t i = 0; i < len; ++i)
        h ^= i&1 ? ~(h<<11^buf[i]^h>>5) : h<<7^buf[i]^h>>3;
    return h;
}
