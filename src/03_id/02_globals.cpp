// ID服务器
// 定义全局变量
//
#include "01_globals.h"
//
// 配置信息
//
char* cfg_maddrs; // MySQL地址表
char *cfg_dbuser, *cfg_dbpass, *cfg_dbname;
acl::master_str_tbl cfg_str[] = { // 字符串配置表
    {"mysql_addrs", "127.0.0.1", &cfg_maddrs},
    {"mysql_user", "root", &cfg_dbuser},
    {"mysql_password", "123456", &cfg_dbpass},
    {"mysql_database", "tnv_idsdb", &cfg_dbname},
    {0, 0, 0}};

int cfg_mtimeout;  // MySQL读写超时
int cfg_maxoffset; // 最大偏移
acl::master_int_tbl cfg_int[] = { // 整型配置表
    {"mysql_rw_timeout",  30, &cfg_mtimeout,  0, 0},
    {"idinc_max_step",   100, &cfg_maxoffset, 0, 0},
    {0, 0, 0, 0, 0}};

std::vector<std::string> g_maddrs; // MySQL地址表
std::string g_hostname; // 主机名
std::vector<id_pair_t> g_ids; // ID表
pthread_mutex_t g_mutex = PTHREAD_MUTEX_INITIALIZER; // 互斥锁
