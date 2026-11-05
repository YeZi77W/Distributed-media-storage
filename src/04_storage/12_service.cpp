#include <vector>
// 存储服务器
// 实现业务服务类
//
#include <linux/limits.h>
#include <atomic>
#include <algorithm>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include "02_proto.h"
#include "03_util.h"
#include "01_globals.h"
#include "05_db.h"
#include "07_file.h"
#include "09_id.h"
#include "11_service.h"
#include "18_volume.h"
#include "19_lifecycle.h"
#include "22_hash.h"

namespace {

bool parse_identity(char const* body, char* appid, char* userid,
    char* fileid) {
    char const* app = body;
    char const* user = body + APPID_SIZE;
    char const* file = user + USERID_SIZE;
    if (!memchr(app, '\0', APPID_SIZE) ||
        !memchr(user, '\0', USERID_SIZE) ||
        !memchr(file, '\0', FILEID_SIZE) ||
        !*app || !*user || !*file || strlen(user) > 128)
        return false;
    memcpy(appid, app, APPID_SIZE);
    memcpy(userid, user, USERID_SIZE);
    memcpy(fileid, file, FILEID_SIZE);
    return true;
}

int ensure_directory(char const* path) {
    if (!mkdir(path, 0755))
        return sync_file_parent(path);
    int error = errno;
    struct stat info;
    if (error == EEXIST && !lstat(path, &info) && S_ISDIR(info.st_mode))
        return OK;
    logger_error("create shared storage directory fail: %s, path: %s",
        strerror(error), path);
    return ERROR;
}

} // namespace

// 业务处理
bool service_c::business(acl::socket_stream* conn,
    char const* head) const {
    // |包体长度|命令|状态|  包体  |
    // |    8   |  1 |  1 |包体长度|
    // 解析包头
    long long bodylen = ntoll(head); // 包体长度
    if (bodylen < 0) {
        error(conn, -1, "invalid body length: %lld < 0", bodylen);
        return false;
    }
    int command = head[BODYLEN_SIZE]; // 命令
    int status = head[BODYLEN_SIZE+COMMAND_SIZE]; // 状态
    logger("bodylen: %lld, command: %d, status: %d",
        bodylen, command, status);

    bool result;

    // 根据命令执行具体业务处理
    switch (command) {
        case CMD_STORAGE_UPLOAD:
            // 处理来自客户机的上传文件请求
            result = upload(conn, bodylen);
            break;

        case CMD_STORAGE_UPLOAD_INIT:
            result = upload_init(conn, bodylen);
            break;
        case CMD_STORAGE_UPLOAD_STATUS:
            result = upload_status(conn, bodylen);
            break;
        case CMD_STORAGE_UPLOAD_CHUNK:
            result = upload_chunk(conn, bodylen);
            break;
        case CMD_STORAGE_UPLOAD_COMMIT:
            result = upload_commit(conn, bodylen);
            break;

        case CMD_STORAGE_FILESIZE:
            // 处理来自客户机的询问文件大小请求
            result = filesize(conn, bodylen);
            break;

        case CMD_STORAGE_FILEMETA:
            result = filemeta(conn, bodylen);
            break;

        case CMD_STORAGE_DOWNLOAD:
            // 处理来自客户机的下载文件请求
            result = download(conn, bodylen);
            break;

        case CMD_STORAGE_DELETE:
            // 处理来自客户机的删除文件请求
            result = del(conn, bodylen);
            break;

        default:
            error(conn, -1, "unknown command: %d", command);
            return false;
    }

    return result;
}

////////////////////////////////////////////////////////////////////////

// 处理来自客户机的上传文件请求
bool service_c::upload(acl::socket_stream* conn,
    long long bodylen) const {
    // |包体长度|命令|状态|应用ID|用户ID|文件ID|文件大小|文件内容|
    // |    8   |  1 |  1 |  16  |  256 |  128 |    8   |文件大小|
    // 检查包体长度
    long long expected = APPID_SIZE + USERID_SIZE +FILEID_SIZE +
        BODYLEN_SIZE;
    if (bodylen < expected) {
        error(conn, -1, "invalid body length: %lld < %lld",
            bodylen, expected);
        return false;
    }

    // 接收包体
    std::vector<char> body_buffer(static_cast<size_t>(expected) + 1, 0);
    char* body = body_buffer.data();
    if (conn->read(body, expected) < 0) {
        logger_error("read fail: %s, expected: %lld, from: %s",
            acl::last_serror(), expected, conn->get_peer());
        return false;
    }

    // 解析包体
    char appid[APPID_SIZE];
    char userid[USERID_SIZE];
    char fileid[FILEID_SIZE];
    if (!parse_identity(body, appid, userid, fileid)) {
        error(conn, -1, "invalid upload identity fields");
        return false;
    }
    long long filesize = ntoll(
        body + APPID_SIZE + USERID_SIZE + FILEID_SIZE);

    // 检查文件大小
    if (filesize != bodylen - expected) {
        logger_error("invalid file size: %lld != %lld",
            filesize, bodylen - expected);
        error(conn, -1, "invalid file size: %lld != %lld",
            filesize, bodylen - expected);
        return false;
    }

    // 生成文件路径
    char filepath[PATH_MAX+1];
    if (genpath(filepath) != OK) {
        error(conn, -1, "get filepath fail");
        return false;
    }

    logger("upload file, appid: %s, userid: %s, "
        "fileid: %s, filesize: %lld, filepath: %s",
        appid, userid, fileid, filesize, filepath);

    // 接收并保存文件
    int result = save(conn, appid, userid, fileid, filesize, filepath);
    if (result == SOCKET_ERROR)
        return false;
    else if (result == ERROR) {
        error(conn, -1, "receive and save file fail, fileid: %s",
            fileid);
        return false;
    }

    return ok(conn);
}

// 处理来自客户机的询问文件大小请求
bool service_c::filesize(acl::socket_stream* conn,
    long long bodylen) const {
    // |包体长度|命令|状态|应用ID|用户ID|文件ID|
    // |    8   |  1 |  1 |  16  |  256 |  128 |
    // 检查包体长度
    long long expected = APPID_SIZE + USERID_SIZE + FILEID_SIZE;
    if (bodylen != expected) {
        error(conn, -1, "invalid body length: %lld != %lld",
            bodylen, expected);
        return false;
    }

    // 接收包体
    std::vector<char> body_buffer(static_cast<size_t>(bodylen) + 1, 0);
    char* body = body_buffer.data();
    if (conn->read(body, bodylen) < 0) {
        logger_error("read fail: %s, bodylen: %lld, from: %s",
            acl::last_serror(), bodylen, conn->get_peer());
        return false;
    }

    // 解析包体
    char appid[APPID_SIZE];
    char userid[USERID_SIZE];
    char fileid[FILEID_SIZE];
    if (!parse_identity(body, appid, userid, fileid)) {
        error(conn, -1, "invalid filesize identity fields");
        return false;
    }

    db_c db; // 数据库访问对象

    // 连接数据库
    if (db.connect() != OK)
        return false;

    std::string filepath; // 文件路径
    long long   filesize; // 文件大小

    // 根据文件ID获取其对应的路径及大小
    if (db.get(appid, userid, fileid, filepath, &filesize) != OK) {
        error(conn, -1, "read database fail, fileid: %s", fileid);
        return false;
    }
    if (!shared_path_ready(filepath.c_str())) {
        error(conn, -1, "shared storage unavailable, fileid: %s", fileid);
        return false;
    }
    if (check_file_size(filepath.c_str(), filesize) != OK) {
        error(conn, -1, "stored file is missing or damaged, fileid: %s", fileid);
        return false;
    }

    logger("filesize, appid: %s, userid: %s, "
        "fileid: %s, filepath: %s, filesize: %lld",
        appid, userid, fileid, filepath.c_str(), filesize);

    // |包体长度|命令|状态|文件大小|
    // |    8   |  1 |  1 |    8   |
    // 构造响应
    bodylen = BODYLEN_SIZE;
    long long resplen = HEADLEN + bodylen;
    std::vector<char> resp_buffer(static_cast<size_t>(resplen) + 1, 0);
    char* resp = resp_buffer.data();
    llton(bodylen, resp);
    resp[BODYLEN_SIZE] = CMD_STORAGE_REPLY;
    resp[BODYLEN_SIZE+COMMAND_SIZE] = 0;
    llton(filesize, resp + HEADLEN);

    // 发送响应
    if (conn->write(resp, resplen) < 0) {
        logger_error("write fail: %s, resplen: %lld, to: %s",
            acl::last_serror(), resplen, conn->get_peer());
        return false;
    }

    return true;
}

// 处理来自客户机的下载文件请求
bool service_c::download(acl::socket_stream* conn,
    long long bodylen) const {
    // |包体长度|命令|状态|应用ID|用户ID|文件ID|偏移|大小|
    // |    8   |  1 |  1 |  16  |  256 |  128 |  8 |  8 |
    // 检查包体长度
    long long expected = APPID_SIZE + USERID_SIZE + FILEID_SIZE +
        BODYLEN_SIZE + BODYLEN_SIZE;
    if (bodylen != expected) {
        error(conn, -1, "invalid body length: %lld != %lld",
            bodylen, expected);
        return false;
    }

    // 接收包体
    std::vector<char> body_buffer(static_cast<size_t>(bodylen) + 1, 0);
    char* body = body_buffer.data();
    if (conn->read(body, bodylen) < 0) {
        logger_error("read fail: %s, bodylen: %lld, from: %s",
            acl::last_serror(), bodylen, conn->get_peer());
        return false;
    }

    // 解析包体
    char appid[APPID_SIZE];
    char userid[USERID_SIZE];
    char fileid[FILEID_SIZE];
    if (!parse_identity(body, appid, userid, fileid)) {
        error(conn, -1, "invalid download identity fields");
        return false;
    }
    long long offset = ntoll(
        body + APPID_SIZE + USERID_SIZE + FILEID_SIZE);
    long long size = ntoll(
        body + APPID_SIZE + USERID_SIZE + FILEID_SIZE + BODYLEN_SIZE);

    db_c db; // 数据库访问对象

    // 连接数据库
    if (db.connect() != OK)
        return false;

    std::string filepath; // 文件路径
    long long   filesize; // 文件大小

    // 根据文件ID获取其对应的路径及大小
    if (db.get(appid, userid, fileid, filepath, &filesize) != OK) {
        error(conn, -1, "read database fail, fileid: %s", fileid);
        return false;
    }
    if (!shared_path_ready(filepath.c_str())) {
        error(conn, -1, "shared storage unavailable, fileid: %s", fileid);
        return false;
    }

    // 检查位置
    if (offset < 0 || filesize < offset) {
        logger_error("invalid offset, %lld is not between 0 and %lld",
            offset, filesize);
        error(conn, -1, "invalid offset, %lld is not between 0 and %lld",
            offset, filesize);
        return false;
    }

    // 大小为零表示下到文件尾
    if (!size)
        size = filesize - offset;

    // 检查大小
    if (size < 0 || filesize - offset < size) {
        logger_error("invalid size, %lld is not between 0 and %lld",
            size, filesize - offset);
        error(conn, -1, "invalid offset, %lld is not between 0 and %lld",
            size, filesize - offset);
        return false;
    }

    logger("download file, appid: %s, userid: %s, fileid: %s, "
        "offset: %lld, size: %lld, filepath: %s, filesize: %lld",
        appid, userid, fileid, offset, size, filepath.c_str(), filesize);

    // 读取并发送文件
    int result = send(conn, filepath.c_str(), offset, size);
    if (result == SOCKET_ERROR)
        return false;
    else if (result == ERROR) {
        error(conn, -1, "read and send file fail, fileid: %s", fileid);
        return false;
    }

    return true;
}

// 处理来自客户机的删除文件请求
bool service_c::del(acl::socket_stream* conn, long long bodylen) const {
    // |包体长度|命令|状态|应用ID|用户ID|文件ID|
    // |    8   |  1 |  1 |  16  |  256 |  128 |
    // 检查包体长度
    long long expected = APPID_SIZE + USERID_SIZE + FILEID_SIZE;
    if (bodylen != expected) {
        error(conn, -1, "invalid body length: %lld != %lld",
            bodylen, expected);
        return false;
    }

    // 接收包体
    std::vector<char> body_buffer(static_cast<size_t>(bodylen) + 1, 0);
    char* body = body_buffer.data();
    if (conn->read(body, bodylen) < 0) {
        logger_error("read fail: %s, bodylen: %lld, from: %s",
            acl::last_serror(), bodylen, conn->get_peer());
        return false;
    }

    // 解析包体
    char appid[APPID_SIZE];
    char userid[USERID_SIZE];
    char fileid[FILEID_SIZE];
    if (!parse_identity(body, appid, userid, fileid)) {
        error(conn, -1, "invalid delete identity fields");
        return false;
    }

    db_c db; // 数据库访问对象

    // 连接数据库
    if (db.connect() != OK)
        return false;

    std::string filepath; // 文件路径
    long long   filesize; // 文件大小

    // 先将元数据标记为待删除；故障后恢复线程可继续物理删除。
    if (db.mark_deleting(appid, userid, fileid, filepath, &filesize) != OK) {
        error(conn, -1, "mark deleting fail, fileid: %s", fileid);
        return false;
    }
    logger("file marked deleting, fileid: %s", fileid);

    if (remove_published_file(filepath.c_str()) != OK) {
        error(conn, -1, "delete file fail, fileid: %s", fileid);
        return false;
    }
    if (db.finish_delete(userid, fileid) != OK) {
        error(conn, -1, "finish delete fail, fileid: %s", fileid);
        return false;
    }

    logger("delete file success, appid: %s, userid: %s, "
        "fileid: %s, filepath: %s, filesize: %lld",
        appid, userid, fileid, filepath.c_str(), filesize);

    return ok(conn);
}

////////////////////////////////////////////////////////////////////////

// 生成文件路径
int service_c::genpath(char* filepath) const {
    // 轮流选择存储卷，保证多个节点使用相同的绝对根路径。
    static std::atomic<unsigned int> next_root(0);
    int nspaths = g_spaths.size();
    if (!nspaths)
        return ERROR;
    int nrand = next_root.fetch_add(1, std::memory_order_relaxed) % nspaths;
    std::string spath = g_spaths[nrand];
    if (!shared_root_ready(spath.c_str()))
        return ERROR;

    // 以存储路径为键从ID服务器获取与之对应的值作为文件ID
    id_c id;
    long fileid = id.get(spath.c_str());
    if (fileid < 0)
        return ERROR;

    return id2path(spath.c_str(), fileid, filepath);
}

// 用文件ID生成文件路径
int service_c::id2path(char const* spath, long fileid,
    char* filepath) const {
    // 检查存储路径
    if (!spath || !shared_root_ready(spath) || fileid < 0) {
        logger_error("storage root or path id is invalid");
        return ERROR;
    }

    // 目录按需创建，文件名包含完整路径ID，避免不同时间或节点覆盖已有文件。
    unsigned long id = static_cast<unsigned long>(fileid);
    unsigned int subdir1 = (id >> 27) & 0x1FF;
    unsigned int subdir2 = (id >> 18) & 0x1FF;
    unsigned int subdir3 = (id >> 9) & 0x1FF;
    char dir1[PATH_MAX + 1], dir2[PATH_MAX + 1], dir3[PATH_MAX + 1];
    int n1 = snprintf(dir1, sizeof(dir1), "%s/%03X", spath, subdir1);
    int n2 = snprintf(dir2, sizeof(dir2), "%s/%03X", dir1, subdir2);
    int n3 = snprintf(dir3, sizeof(dir3), "%s/%03X", dir2, subdir3);
    int nf = snprintf(filepath, PATH_MAX + 1, "%s/%016lX", dir3, id);
    if (n1 < 0 || n1 >= (int)sizeof(dir1) ||
        n2 < 0 || n2 >= (int)sizeof(dir2) ||
        n3 < 0 || n3 >= (int)sizeof(dir3) ||
        nf < 0 || nf >= PATH_MAX + 1) {
        logger_error("generated shared storage path is too long");
        return ERROR;
    }
    if (ensure_directory(dir1) != OK || ensure_directory(dir2) != OK ||
        ensure_directory(dir3) != OK)
        return ERROR;

    return OK;
}

// 接收并保存文件
int service_c::save(acl::socket_stream* conn, char const* appid,
    char const* userid, char const* fileid, long long filesize,
    char const* filepath) const {
    file_c file; // 文件操作对象
    std::string temporary = uploading_path(filepath);
    if (temporary.empty())
        return ERROR;

    // 临时文件只有在完整写入、同步、关闭后才登记待发布状态。
    if (!shared_path_ready(temporary.c_str()))
        return ERROR;
    if (file.open(temporary.c_str(), file_c::O_WRITE) != OK)
        return ERROR;

    // 依次将接收到的数据块写入文件
    long long remain = filesize; // 未接收字节数
    char rcvwr[STORAGE_RCVWR_SIZE]; // 接收写入缓冲区
    while (remain) { // 还有未接收数据
        // 接收数据
        long long bytes = std::min(remain, (long long)sizeof(rcvwr));
        long long count = conn->read(rcvwr, bytes);
        if (count <= 0) {
            logger_error("read fail: %s, bytes: %lld, from: %s",
                acl::last_serror(), bytes, conn->get_peer());
            file.close();
            file_c::del(temporary.c_str());
            return SOCKET_ERROR;
        }
        // 写入文件
        if (file.write(rcvwr, count) != OK) {
            file.close();
            file_c::del(temporary.c_str());
            return ERROR;
        }
        // 未收递减
        remain -= count;
    }

    // 只有数据同步且文件关闭成功后才发布元数据。
    if (file.sync() != OK || file.close() != OK) {
        file.close();
        file_c::del(temporary.c_str());
        return ERROR;
    }
    if (sync_file_parent(temporary.c_str()) != OK) {
        file_c::del(temporary.c_str());
        return ERROR;
    }

    db_c db; // 数据库访问对象

    // 连接数据库
    if (db.connect() != OK) {
        file_c::del(temporary.c_str());
        return ERROR;
    }

    std::string digest;
    if (!file_sha256(temporary.c_str(), filesize, digest) ||
        db.stage_upload(appid, userid, fileid, filepath, filesize,
            digest.c_str()) != OK) {
        file_c::del(temporary.c_str());
        return ERROR;
    }
    logger("upload staged, fileid: %s, path: %s", fileid, filepath);
    // 登记后失败不清理临时文件，交由恢复线程安全地重试发布。
    if (publish_file(filepath, filesize) != OK)
        return ERROR;
    if (db.finish_upload(userid, fileid) != OK)
        return ERROR;
    logger("upload ready, fileid: %s", fileid);

    return OK;
}

// 读取并发送文件
int service_c::send(acl::socket_stream* conn, char const* filepath,
    long long offset, long long size) const {
    file_c file; // 文件操作对象

    // 打开文件
    if (!shared_path_ready(filepath))
        return ERROR;
    if (file.open(filepath, file_c::O_READ) != OK)
        return ERROR;

    // 设置偏移
    if (offset && file.seek(offset) != OK) {
        file.close();
        return ERROR;
    }

    // |包体长度|命令|状态|文件内容|
    // |    8   |  1 |  1 |内容大小|
    // 构造响应头
    long long bodylen = size;
    long long headlen = HEADLEN;
    std::vector<char> head_buffer(static_cast<size_t>(headlen) + 1, 0);
    char* head = head_buffer.data();
    llton(bodylen, head);
    head[BODYLEN_SIZE] = CMD_STORAGE_REPLY;
    head[BODYLEN_SIZE+COMMAND_SIZE] = 0;

    // 发送响应头
    if (conn->write(head, headlen) < 0) {
        logger_error("write fail: %s, headlen: %lld, to: %s",
            acl::last_serror(), headlen, conn->get_peer());
        file.close();
        return SOCKET_ERROR;
    }

    // 依次将从文件中读取到的数据块作为响应体的一部分发送出去
    long long remain = size; // 未读取字节数
    char rdsnd[STORAGE_RDSND_SIZE]; // 读取发送缓冲区
    while (remain) { // 还有未读取数据
        // 读取文件
        long long count = std::min(remain, (long long)sizeof(rdsnd));
        if (file.read(rdsnd, count) != OK) {
            file.close();
            return ERROR;
        }
        // 发送数据
        if (conn->write(rdsnd, count) < 0) {
            logger_error("write fail: %s, count: %lld, to: %s",
                acl::last_serror(), count, conn->get_peer());
            file.close();
            return SOCKET_ERROR;
        }
        // 未读递减
        remain -= count;
    }

    // 关闭文件
    file.close();

    return OK;
}

////////////////////////////////////////////////////////////////////////

// 应答成功
bool service_c::ok(acl::socket_stream* conn) const {
    // |包体长度|命令|状态|
    // |    8   |  1 |  1 |
    // 构造响应
    long long bodylen = 0;
    long long resplen = HEADLEN + bodylen;
    std::vector<char> resp_buffer(static_cast<size_t>(resplen) + 1, 0);
    char* resp = resp_buffer.data();
    llton(bodylen, resp);
    resp[BODYLEN_SIZE] = CMD_STORAGE_REPLY;
    resp[BODYLEN_SIZE+COMMAND_SIZE] = 0;

    // 发送响应
    if (conn->write(resp, resplen) < 0) {
        logger_error("write fail: %s, resplen: %lld, to: %s",
            acl::last_serror(), resplen, conn->get_peer());
        return false;
    }

    return true;
}

// 应答错误
bool service_c::error(acl::socket_stream* conn, short errnumb,
    char const* format, ...) const {
    // 错误描述
    char errdesc[ERROR_DESC_SIZE];
    va_list ap;
    va_start(ap, format);
    vsnprintf(errdesc, ERROR_DESC_SIZE, format, ap);
    va_end(ap);
    logger_error("%s", errdesc);
    acl::string desc;
    desc.format("[%s] %s", g_hostname.c_str(), errdesc);
    memset(errdesc, 0, sizeof(errdesc));
    strncpy(errdesc, desc.c_str(), ERROR_DESC_SIZE - 1);
    size_t desclen = strlen(errdesc);
    desclen += desclen != 0;

    // |包体长度|命令|状态|错误号|错误描述|
    // |    8   |  1 |  1 |   2  | <=1024 |
    // 构造响应
    long long bodylen = ERROR_NUMB_SIZE + desclen;
    long long resplen = HEADLEN + bodylen;
    std::vector<char> resp_buffer(static_cast<size_t>(resplen) + 1, 0);
    char* resp = resp_buffer.data();
    llton(bodylen, resp);
    resp[BODYLEN_SIZE] = CMD_STORAGE_REPLY;
    resp[BODYLEN_SIZE+COMMAND_SIZE] = STATUS_ERROR;
    ston(errnumb, resp + HEADLEN);
    if (desclen)
        strcpy(resp + HEADLEN + ERROR_NUMB_SIZE, errdesc);

    // 发送响应
    if (conn->write(resp, resplen) < 0) {
        logger_error("write fail: %s, resplen: %lld, to: %s",
            acl::last_serror(), resplen, conn->get_peer());
        return false;
    }

    return true;
}
