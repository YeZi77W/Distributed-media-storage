// 断点续传会话持久化；事务由调用方控制。
#include <errno.h>
#include <stdlib.h>
#include "01_types.h"
#include "01_globals.h"
#include "05_db.h"

namespace {
bool number(char const* value, long long& result) {
    if (!value) return false;
    errno = 0;
    char* end = NULL;
    result = strtoll(value, &end, 10);
    return errno != ERANGE && end != value && !*end && result >= 0;
}
}

int db_c::begin(void) const {
    return m_mysql && !mysql_query(m_mysql, "START TRANSACTION") ? OK : ERROR;
}

int db_c::commit(void) const {
    if (m_mysql && !mysql_query(m_mysql, "COMMIT")) return OK;
    if (m_mysql) logger_error("commit upload transaction fail: %s", mysql_error(m_mysql));
    return ERROR;
}

void db_c::rollback(void) const {
    if (m_mysql && mysql_query(m_mysql, "ROLLBACK"))
        logger_error("rollback upload transaction fail: %s", mysql_error(m_mysql));
}

int db_c::create_session(upload_session_t const& session) const {
    std::string app, user, file, path, digest;
    if (!escape(session.appid.c_str(), app) ||
        !escape(session.userid.c_str(), user) ||
        !escape(session.fileid.c_str(), file) ||
        !escape(session.filepath.c_str(), path) ||
        !escape(session.digest.c_str(), digest) || session.filesize < 0)
        return ERROR;
    acl::string sql;
    sql.format("INSERT INTO t_upload_session "
        "(fileid,appid,userid,file_path,file_size,sha256) "
        "VALUES ('%s','%s','%s','%s',%lld,'%s');",
        file.c_str(), app.c_str(), user.c_str(), path.c_str(),
        session.filesize, digest.c_str());
    if (mysql_query(m_mysql, sql.c_str())) {
        logger_error("create upload session fail: %s", mysql_error(m_mysql));
        return ERROR;
    }
    return OK;
}

int db_c::get_session(char const* appid, char const* userid,
    char const* fileid, upload_session_t& session, bool for_update) const {
    std::string app, user, file;
    if (!escape(appid, app) || !escape(userid, user) || !escape(fileid, file))
        return ERROR;
    acl::string sql;
    sql.format("SELECT file_path,file_size,committed_offset,sha256,status,"
        "(update_time < NOW() - INTERVAL 1 DAY) "
        "FROM t_upload_session WHERE fileid='%s' AND appid='%s' "
        "AND userid='%s'%s;", file.c_str(), app.c_str(), user.c_str(),
        for_update ? " FOR UPDATE" : "");
    if (mysql_query(m_mysql, sql.c_str())) {
        logger_error("query upload session fail: %s", mysql_error(m_mysql));
        return ERROR;
    }
    MYSQL_RES* result = mysql_store_result(m_mysql);
    if (!result) return ERROR;
    MYSQL_ROW row = mysql_fetch_row(result);
    long long size = 0, offset = 0, status = 0, expired = 0;
    if (!row || !row[0] || !row[3] || !number(row[1], size) ||
        !number(row[2], offset) || !number(row[4], status) ||
        !number(row[5], expired) || expired > 1 ||
        offset > size || status > UPLOAD_DONE) {
        mysql_free_result(result);
        return ERROR;
    }
    session.appid = appid;
    session.userid = userid;
    session.fileid = fileid;
    session.filepath = row[0];
    session.digest = row[3];
    session.filesize = size;
    session.offset = offset;
    session.expired = expired != 0;
    session.status = static_cast<upload_status_t>(status);
    mysql_free_result(result);
    return OK;
}

int db_c::advance_session(char const* fileid, long long old_offset,
    long long new_offset) const {
    std::string file;
    if (!escape(fileid, file) || old_offset < 0 || new_offset <= old_offset)
        return ERROR;
    acl::string sql;
    sql.format("UPDATE t_upload_session SET committed_offset=%lld "
        "WHERE fileid='%s' AND status=%d AND committed_offset=%lld "
        "AND file_size>=%lld;", new_offset, file.c_str(), UPLOAD_ACTIVE,
        old_offset, new_offset);
    if (mysql_query(m_mysql, sql.c_str())) return ERROR;
    return mysql_affected_rows(m_mysql) == 1 ? OK : ERROR;
}

int db_c::set_session_status(char const* fileid, upload_status_t from,
    upload_status_t to) const {
    std::string file;
    if (!escape(fileid, file)) return ERROR;
    acl::string sql;
    sql.format("UPDATE t_upload_session SET status=%d WHERE fileid='%s' "
        "AND status=%d;", to, file.c_str(), from);
    if (mysql_query(m_mysql, sql.c_str())) return ERROR;
    return mysql_affected_rows(m_mysql) == 1 ? OK : ERROR;
}

int db_c::remove_session(char const* fileid) const {
    std::string file;
    if (!escape(fileid, file)) return ERROR;
    acl::string sql;
    sql.format("DELETE FROM t_upload_session WHERE fileid='%s'", file.c_str());
    return mysql_query(m_mysql, sql.c_str()) ? ERROR : OK;
}

int db_c::expired_sessions(std::vector<upload_session_t>& sessions) const {
    if (mysql_query(m_mysql, "SELECT fileid,appid,userid FROM t_upload_session "
        "WHERE status IN (0,2) AND update_time < NOW() - INTERVAL 1 DAY "
        "ORDER BY update_time LIMIT 100")) return ERROR;
    MYSQL_RES* result = mysql_store_result(m_mysql);
    if (!result) return ERROR;
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result))) if (row[0] && row[1] && row[2]) {
        upload_session_t session;
        session.fileid = row[0];
        session.appid = row[1];
        session.userid = row[2];
        sessions.push_back(session);
    }
    bool good = !mysql_errno(m_mysql);
    mysql_free_result(result);
    return good ? OK : ERROR;
}
