// 跟踪服务器
// 实现数据库访问类
//
#include "01_globals.h"
#include "03_cache.h"
#include "05_db.h"

// 构造函数
db_c::db_c(void): m_mysql(mysql_init(NULL)) { // 创建MySQL对象
    if (!m_mysql)
        logger_error("mysql_init returned null");
}

// 析构函数
db_c::~db_c(void) {
    // 销毁MySQL对象
    if (m_mysql) {
        mysql_close(m_mysql);
        m_mysql = NULL;
    }
}

// 连接数据库
int db_c::connect(void) {
    if (!m_mysql) return ERROR;
    unsigned int timeout = cfg_mtimeout > 0 ? cfg_mtimeout : 3;
    mysql_options(m_mysql, MYSQL_OPT_CONNECT_TIMEOUT, &timeout);
    mysql_options(m_mysql, MYSQL_OPT_READ_TIMEOUT, &timeout);
    mysql_options(m_mysql, MYSQL_OPT_WRITE_TIMEOUT, &timeout);
    MYSQL* mysql = m_mysql;

    // 遍历MySQL地址表，尝试连接数据库
    for (std::vector<std::string>::const_iterator maddr =
        g_maddrs.begin(); maddr != g_maddrs.end(); ++maddr)
        if ((m_mysql = mysql_real_connect(mysql, maddr->c_str(),
            cfg_dbuser, cfg_dbpass, cfg_dbname, 0, NULL, 0)))
            return OK;

    logger_error("connect database fail: %s",
        mysql_error(m_mysql = mysql));
    return ERROR;
}

namespace {
std::string escaped(MYSQL* db, const char* text) {
    std::vector<char> buf(strlen(text)*2+1);
    mysql_real_escape_string(db, buf.data(), text, strlen(text));
    return buf.data();
}
}
int db_c::get(char const* appid, char const* userid, std::string& groupname) const {
    groupname.clear();
    std::string sql = "SELECT group_name FROM t_router WHERE appid='" + escaped(m_mysql, appid) + "' AND userid='" + escaped(m_mysql, userid) + "'";
    if (mysql_query(m_mysql, sql.c_str())) return ERROR;
    MYSQL_RES* res = mysql_store_result(m_mysql);
    if (!res) return ERROR;
    MYSQL_ROW row = mysql_fetch_row(res);
    if (row && row[0]) groupname = row[0];
    mysql_free_result(res);
    return OK;
}
int db_c::set(char const* appid, char const* userid, char const* groupname) const {
    std::string sql = "INSERT INTO t_router(appid,userid,group_name) VALUES('" + escaped(m_mysql, appid) + "','" + escaped(m_mysql, userid) + "','" + escaped(m_mysql, groupname) + "')";
    if (!mysql_query(m_mysql, sql.c_str())) return OK;
    // Only a duplicate identity can be resolved by reading the winning route.
    return mysql_errno(m_mysql) == 1062 ? OK : ERROR;
}

// 获取全部组名
int db_c::get(std::vector<std::string>& groupnames) const {
    // 查询全部组名
    acl::string sql;
    sql.format("SELECT group_name FROM t_groups_info;");
    if (mysql_query(m_mysql, sql.c_str())) {
        logger_error("query database fail: %s, sql: %s",
            mysql_error(m_mysql), sql.c_str());
        return ERROR;
    }

    // 获取查询结果
    MYSQL_RES* res = mysql_store_result(m_mysql);
    if (!res) {
        logger_error("result is null: %s, sql: %s",
            mysql_error(m_mysql), sql.c_str());
        return ERROR;
    }

    // 获取结果记录
    int nrows = mysql_num_rows(res);
    for (int i = 0; i < nrows; ++i) {
        MYSQL_ROW row = mysql_fetch_row(res);
        if (!row)
            break;
        groupnames.push_back(row[0]);
    }

    mysql_free_result(res);
    return OK;
}
