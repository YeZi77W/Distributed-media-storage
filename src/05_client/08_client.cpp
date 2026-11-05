#include "client.h"
#include <memory>
#include <mutex>
namespace { std::mutex storage_pool_mutex; }
namespace { std::unique_ptr<tnv::RpcControlClient> control_client; }
// 客户机
// 实现客户机类
//
#include <lib_acl.h>
#include "01_types.h"
#include "03_util.h"
#include "01_conn.h"
#include "03_pool.h"
#include "05_mngr.h"
#include "07_client.h"

#define MAX_SOCKERRS 10 // 套接字通信错误最大次数

acl::connect_manager* client_c::s_mngr = NULL;
std::vector<std::string> client_c::s_taddrs;
int client_c::s_scount = 8;

// 初始化
int client_c::init(char const* taddrs,
    int tcount /* = 16 */, int scount /* = 8 */) {
    if (s_mngr)
        return OK;

    // 跟踪服务器地址表
    if (!taddrs || !*taddrs) {
        logger_error("tracker addresses is null");
        return ERROR;
    }
    split(taddrs, s_taddrs);
    if (s_taddrs.empty()) {
        logger_error("tracker addresses is empty");
        return ERROR;
    }

    // 跟踪服务器连接数上限
    if (tcount <= 0) {
        logger_error("invalid tracker connection pool count %d <= 0",
            tcount);
        return ERROR;
    }

    // 存储服务器连接数上限
    if (scount <= 0) {
        logger_error("invalid storage connection pool count %d <= 0",
            scount);
        return ERROR;
    }
    s_scount = scount;

    // 创建连接池管理器
    if (!(s_mngr = new mngr_c)) {
        logger_error("create connection pool manager fail: %s",
            acl_last_serror());
        return ERROR;
    }

    // 初始化跟踪服务器连接池
    try { control_client = std::make_unique<tnv::RpcControlClient>(s_taddrs.front()); }
    catch (...) { delete s_mngr; s_mngr = NULL; return ERROR; }

    return OK;
}

// 终结化
void client_c::deinit(void) {
    (void)deinit_checked();
}

bool client_c::deinit_checked(void) {
    bool clean = true;
    if (control_client) {
        // A false clean() is safe to release only after stop() has joined owner.
        // If stop throws, keep the management anchor and propagate the failure.
        control_client->stop();
        clean = control_client->clean();
        control_client.reset();
    }
    if (s_mngr) {
        delete s_mngr;
        s_mngr = NULL;
    }

    s_taddrs.clear();
    return clean;
}

// 从跟踪服务器获取存储服务器地址列表
int client_c::saddrs(char const* appid, char const* userid,
    char const* fileid, std::string& saddrs) {
    if (!control_client) return ERROR;
    tnv::control::NodesRequest q; q.set_appid(appid);q.set_userid(userid);q.set_fileid(fileid);
    auto r=control_client->call("GetStorageNodes",q);
    if(r.error!=tnv::rpc::RpcErrorCode::none||!tnv::valid_nodes(r.nodes)) return ERROR;
    saddrs.clear();
    for(const auto& n:r.nodes.nodes()) if(n.node_id()==preferred_node_) saddrs+=n.data_ip()+":"+std::to_string(n.data_port())+";";
    for(const auto& n:r.nodes.nodes()) if(n.node_id()!=preferred_node_) saddrs+=n.data_ip()+":"+std::to_string(n.data_port())+";";
    return OK;
}

// 从跟踪服务器获取组列表
int client_c::groups(std::string& groups) {
    if (!control_client) return ERROR;
    tnv::control::GroupsRequest q;auto r=control_client->call("ListGroups",q);
    if(r.error!=tnv::rpc::RpcErrorCode::none||r.groups.code()!=tnv::control::OK)return ERROR;
    groups=tnv::format_groups(r.groups);return OK;
}

// 向存储服务器上传文件
int client_c::upload(char const* appid, char const* userid,
    char const* fileid, char const* filepath) {
    return upload_resumable(appid, userid, fileid, filepath);
}

// 向存储服务器上传文件
int client_c::upload(char const* appid, char const* userid,
    char const* fileid, char const* filedata, long long filesize) {
    // 检查参数
    if (!appid || !*appid) {
        logger_error("appid is null");
        return ERROR;
    }
    if (!userid || !*userid) {
        logger_error("userid is null");
        return ERROR;
    }
    if (!fileid || !*fileid) {
        logger_error("fileid is null");
        return ERROR;
    }
    if (!filedata || !filesize) {
        logger_error("filedata is null");
        return ERROR;
    }

    // 从跟踪服务器获取存储服务器地址列表
    int result;
    std::string ssaddrs;
    if ((result = saddrs(appid, userid, fileid, ssaddrs)) != OK)
        return result;
    std::vector<std::string> vsaddrs;
    split(ssaddrs.c_str(), vsaddrs);
    if (vsaddrs.empty()) {
        logger_error("storage addresses is empty");
        return ERROR;
    }

    result = ERROR;

    for (std::vector<std::string>::const_iterator saddr =
        vsaddrs.begin(); saddr != vsaddrs.end(); ++saddr) {
        // 获取存储服务器连接池
        std::unique_lock<std::mutex> pool_guard(storage_pool_mutex);
        pool_c* spool = (pool_c*)s_mngr->get(saddr->c_str());
        if (!spool) {
            s_mngr->set(saddr->c_str(), s_scount);
            if (!(spool = (pool_c*)s_mngr->get(saddr->c_str()))) {
                logger_warn(
                    "storage connection pool is null, saddr: %s",
                    saddr->c_str());
                continue;
            }
        }

        pool_guard.unlock();
        for (int sockerrs = 0; sockerrs < MAX_SOCKERRS; ++sockerrs) {
            // 获取存储服务器连接
            conn_c* sconn = (conn_c*)spool->peek();
            if (!sconn) {
                logger_warn("storage connection is null, saddr: %s",
                    saddr->c_str());
                break;
            }

            // 向存储服务器上传文件
            result = sconn->upload(
                appid, userid, fileid, filedata, filesize);

            if (result == SOCKET_ERROR) {
                logger_warn("upload file fail: %s", sconn->errdesc());
                spool->put(sconn, false);
            }
            else {
                if (result == OK)
                    spool->put(sconn, true);
                else {
                    logger_error("upload file fail: %s",
                        sconn->errdesc());
                    spool->put(sconn, false);
                }
                return result;
            }
        }
    }

    return result;
}

// 向存储服务器询问文件大小
int client_c::filesize(char const* appid, char const* userid,
    char const* fileid, long long* filesize) {
    // 检查参数
    if (!appid || !*appid) {
        logger_error("appid is null");
        return ERROR;
    }
    if (!userid || !*userid) {
        logger_error("userid is null");
        return ERROR;
    }
    if (!fileid || !*fileid) {
        logger_error("fileid is null");
        return ERROR;
    }

    // 从跟踪服务器获取存储服务器地址列表
    int result;
    std::string ssaddrs;
    if ((result = saddrs(appid, userid, fileid, ssaddrs)) != OK)
        return ERROR;
    std::vector<std::string> vsaddrs;
    split(ssaddrs.c_str(), vsaddrs);
    if (vsaddrs.empty()) {
        logger_error("storage addresses is empty");
        return ERROR;
    }

    result = ERROR;

    for (std::vector<std::string>::const_iterator saddr =
        vsaddrs.begin(); saddr != vsaddrs.end(); ++saddr) {
        // 获取存储服务器连接池
        std::unique_lock<std::mutex> pool_guard(storage_pool_mutex);
        pool_c* spool = (pool_c*)s_mngr->get(saddr->c_str());
        if (!spool) {
            s_mngr->set(saddr->c_str(), s_scount);
            if (!(spool = (pool_c*)s_mngr->get(saddr->c_str()))) {
                logger_warn(
                    "storage connection pool is null, saddr: %s",
                    saddr->c_str());
                continue;
            }
        }

        pool_guard.unlock();
        for (int sockerrs = 0; sockerrs < MAX_SOCKERRS; ++sockerrs) {
            // 获取存储服务器连接
            conn_c* sconn = (conn_c*)spool->peek();
            if (!sconn) {
                logger_warn("storage connection is null, saddr: %s",
                    saddr->c_str());
                break;
            }

            // 向存储服务器询问文件大小
            result = sconn->filesize(appid, userid, fileid, filesize);

            if (result == SOCKET_ERROR) {
                logger_warn("get filesize fail: %s", sconn->errdesc());
                spool->put(sconn, false);
            }
            else {
                if (result == OK)
                    spool->put(sconn, true);
                else {
                    logger_error("get filesize fail: %s",
                        sconn->errdesc());
                    spool->put(sconn, false);
                }
                return result;
            }
        }
    }

    return result;
}

// 从存储服务器下载文件
int client_c::download(char const* appid, char const* userid,
    char const* fileid, long long offset, long long size,
    char** filedata, long long* filesize) {
    // 检查参数
    if (!appid || !*appid) {
        logger_error("appid is null");
        return ERROR;
    }
    if (!userid || !*userid) {
        logger_error("userid is null");
        return ERROR;
    }
    if (!fileid || !*fileid) {
        logger_error("fileid is null");
        return ERROR;
    }

    // 从跟踪服务器获取存储服务器地址列表
    int result;
    std::string ssaddrs;
    if ((result = saddrs(appid, userid, fileid, ssaddrs)) != OK)
        return ERROR;
    std::vector<std::string> vsaddrs;
    split(ssaddrs.c_str(), vsaddrs);
    if (vsaddrs.empty()) {
        logger_error("storage addresses is empty");
        return ERROR;
    }

    result = ERROR;

    for (std::vector<std::string>::const_iterator saddr =
        vsaddrs.begin(); saddr != vsaddrs.end(); ++saddr) {
        // 获取存储服务器连接池
        std::unique_lock<std::mutex> pool_guard(storage_pool_mutex);
        pool_c* spool = (pool_c*)s_mngr->get(saddr->c_str());
        if (!spool) {
            s_mngr->set(saddr->c_str(), s_scount);
            if (!(spool = (pool_c*)s_mngr->get(saddr->c_str()))) {
                logger_warn(
                    "storage connection pool is null, saddr: %s",
                    saddr->c_str());
                continue;
            }
        }

        pool_guard.unlock();
        for (int sockerrs = 0; sockerrs < MAX_SOCKERRS; ++sockerrs) {
            // 获取存储服务器连接
            conn_c* sconn = (conn_c*)spool->peek();
            if (!sconn) {
                logger_warn("storage connection is null, saddr: %s",
                    saddr->c_str());
                break;
            }

            // 从存储服务器下载文件
            result = sconn->download(
                appid, userid, fileid, offset, size, filedata, filesize);

            if (result == SOCKET_ERROR) {
                logger_warn("download file fail: %s", sconn->errdesc());
                spool->put(sconn, false);
            }
            else {
                if (result == OK)
                    spool->put(sconn, true);
                else {
                    logger_error("download file fail: %s",
                        sconn->errdesc());
                    spool->put(sconn, false);
                }
                return result;
            }
        }
    }

    return result;
}

// 删除存储服务器上的文件
int client_c::del(char const* appid, char const* userid,
    char const* fileid) {
    // 检查参数
    if (!appid || !*appid) {
        logger_error("appid is null");
        return ERROR;
    }
    if (!userid || !*userid) {
        logger_error("userid is null");
        return ERROR;
    }
    if (!fileid || !*fileid) {
        logger_error("fileid is null");
        return ERROR;
    }

    // 从跟踪服务器获取存储服务器地址列表
    int result;
    std::string ssaddrs;
    if ((result = saddrs(appid, userid, fileid, ssaddrs)) != OK)
        return ERROR;
    std::vector<std::string> vsaddrs;
    split(ssaddrs.c_str(), vsaddrs);
    if (vsaddrs.empty()) {
        logger_error("storage addresses is empty");
        return ERROR;
    }

    result = ERROR;

    for (std::vector<std::string>::const_iterator saddr =
        vsaddrs.begin(); saddr != vsaddrs.end(); ++saddr) {
        // 获取存储服务器连接池
        std::unique_lock<std::mutex> pool_guard(storage_pool_mutex);
        pool_c* spool = (pool_c*)s_mngr->get(saddr->c_str());
        if (!spool) {
            s_mngr->set(saddr->c_str(), s_scount);
            if (!(spool = (pool_c*)s_mngr->get(saddr->c_str()))) {
                logger_warn(
                    "storage connection pool is null, saddr: %s",
                    saddr->c_str());
                continue;
            }
        }

        pool_guard.unlock();
        for (int sockerrs = 0; sockerrs < MAX_SOCKERRS; ++sockerrs) {
            // 获取存储服务器连接
            conn_c* sconn = (conn_c*)spool->peek();
            if (!sconn) {
                logger_warn("storage connection is null, saddr: %s",
                    saddr->c_str());
                break;
            }

            // 删除存储服务器上的文件
            result = sconn->del(appid, userid, fileid);

            if (result == SOCKET_ERROR) {
                logger_warn("delete file fail: %s", sconn->errdesc());
                spool->put(sconn, false);
            }
            else {
                if (result == OK)
                    spool->put(sconn, true);
                else {
                    logger_error("delete file fail: %s",
                        sconn->errdesc());
                    spool->put(sconn, false);
                }
                return result;
            }
        }
    }

    return result;
}
