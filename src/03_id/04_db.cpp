// ID服务器
// 实现数据库访问类
//
#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <mysqld_error.h>
#include "01_globals.h"
#include "03_db.h"

namespace {

// 首次创建同一个键时可能发生主键冲突或死锁，重新开启事务即可重试。
const int MAX_ALLOC_RETRIES = 5;

bool retryable(unsigned int error) {
    return error == ER_DUP_ENTRY || error == ER_LOCK_DEADLOCK ||
        error == ER_LOCK_WAIT_TIMEOUT;
}

} // namespace

// 构造函数
db_c::db_c(void): m_mysql(mysql_init(NULL)) { // 创建MySQL对象
    if (!m_mysql)
        logger_error("create dao fail: mysql_init returned null");
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
    if (!m_mysql)
        return ERROR;

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

// 原子地预留一个号段，返回号段的首个ID。
int db_c::get(char const* key, int inc, long* value) const {
    if (!m_mysql || !key || !*key || !value || inc <= 0) {
        logger_error("invalid id allocation arguments");
        return ERROR;
    }

    size_t keylen = strlen(key);
    if (keylen > ID_KEY_MAX) {
        logger_error("id key too long: %lu > %d", keylen, ID_KEY_MAX);
        return ERROR;
    }

    // 存储路径会成为ID键，先转义再拼入SQL。
    char escaped[ID_KEY_MAX * 2 + 1];
    if (mysql_real_escape_string(m_mysql, escaped, key, keylen) ==
        (unsigned long)-1) {
        logger_error("escape id key fail: %s", mysql_error(m_mysql));
        return ERROR;
    }

    if (mysql_autocommit(m_mysql, 0)) {
        logger_error("start id allocation transaction fail: %s",
            mysql_error(m_mysql));
        return ERROR;
    }

    bool committed = false;
    bool rollback_failed = false;
    long start = -1;

    for (int attempt = 0; attempt < MAX_ALLOC_RETRIES; ++attempt) {
        unsigned int failure = 0;
        long current = 0;
        bool found = false;

        do {
            // 行锁跨进程生效，后续实例必须等当前事务结束才能读取旧值。
            acl::string sql;
            sql.format("SELECT id_value FROM t_id_gen WHERE id='%s' FOR UPDATE;",
                escaped);
            if (mysql_query(m_mysql, sql.c_str())) {
                failure = mysql_errno(m_mysql);
                logger_error("lock id row fail: %s, key: %s",
                    mysql_error(m_mysql), key);
                break;
            }

            MYSQL_RES* res = mysql_store_result(m_mysql);
            if (!res) {
                failure = mysql_errno(m_mysql);
                logger_error("read id row fail: %s, key: %s",
                    mysql_error(m_mysql), key);
                break;
            }

            MYSQL_ROW row = mysql_fetch_row(res);
            if (row) {
                found = true;
                if (!row[0]) {
                    logger_error("id value is null, key: %s", key);
                    mysql_free_result(res);
                    break;
                }

                errno = 0;
                char* end = NULL;
                current = strtol(row[0], &end, 10);
                if (errno == ERANGE || end == row[0] || *end ||
                    current < 0 || current > LONG_MAX - inc) {
                    logger_error("invalid or exhausted id value, key: %s, value: %s",
                        key, row[0]);
                    mysql_free_result(res);
                    break;
                }
            }
            else if (mysql_errno(m_mysql)) {
                failure = mysql_errno(m_mysql);
                logger_error("fetch id row fail: %s, key: %s",
                    mysql_error(m_mysql), key);
                mysql_free_result(res);
                break;
            }
            mysql_free_result(res);

            if (found) {
                sql.format("UPDATE t_id_gen SET id_value=id_value+%d "
                    "WHERE id='%s';", inc, escaped);
            }
            else {
                // 首次建键的竞争由主键约束裁决，失败者回滚后重试。
                sql.format("INSERT INTO t_id_gen (id, id_value) "
                    "VALUES ('%s', %d);", escaped, inc);
            }

            if (mysql_query(m_mysql, sql.c_str())) {
                failure = mysql_errno(m_mysql);
                logger_error("reserve id range fail: %s, key: %s",
                    mysql_error(m_mysql), key);
                break;
            }
            if (found && mysql_affected_rows(m_mysql) != 1) {
                logger_error("id row update count is not one, key: %s", key);
                break;
            }

            // 只有提交被确认后才把号段交给调用方；提交不确定时丢弃号段。
            if (mysql_commit(m_mysql)) {
                failure = mysql_errno(m_mysql);
                logger_error("commit id range fail: %s, key: %s",
                    mysql_error(m_mysql), key);
                break;
            }

            start = current;
            committed = true;
        } while (false);

        if (committed)
            break;

        if (mysql_rollback(m_mysql)) {
            logger_error("rollback id range fail: %s, key: %s",
                mysql_error(m_mysql), key);
            rollback_failed = true;
            break;
        }
        if (!retryable(failure) || attempt + 1 == MAX_ALLOC_RETRIES)
            break;

        logger_warn("retry id allocation, key: %s, attempt: %d",
            key, attempt + 1);
    }

    // 回滚失败时关闭连接，不冒险让恢复自动提交提交未确认的事务。
    if (rollback_failed)
        return ERROR;

    if (mysql_autocommit(m_mysql, 1)) {
        logger_error("restore autocommit fail: %s, key: %s",
            mysql_error(m_mysql), key);
        return ERROR;
    }

    if (!committed)
        return ERROR;

    *value = start;
    return OK;
}
