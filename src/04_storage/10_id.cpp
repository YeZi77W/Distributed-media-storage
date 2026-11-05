#include <vector>
// 存储服务器
// 实现ID客户机类
//
#include <atomic>
#include <lib_acl.hpp>
#include "02_proto.h"
#include "03_util.h"
#include "01_globals.h"
#include "09_id.h"

// 从ID服务器获取与ID键相对应的值
long id_c::get(char const* key) const {
    // 检查ID键
    if (!key) {
        logger_error("key is null");
        return -1;
    }
    size_t keylen = strlen(key);
    if (!keylen) {
        logger_error("key is null");
        return -1;
    }
    if (keylen > ID_KEY_MAX) {
        logger_error("key too big: %lu > %d", keylen, ID_KEY_MAX);
        return -1;
    }

    // |包体长度|命令|状态|  ID键  |
    // |    8   |  1 |  1 |包体长度|
    // 构造请求
    long long bodylen = keylen + 1;
    long long requlen = HEADLEN + bodylen;
    std::vector<char> requ_buffer(static_cast<size_t>(requlen) + 1, 0);
    char* requ = requ_buffer.data();
    llton(bodylen, requ);
    requ[BODYLEN_SIZE] = CMD_ID_GET;
    requ[BODYLEN_SIZE+COMMAND_SIZE] = 0;
    strcpy(requ + HEADLEN, key);

    // 向ID服务器发送请求，接收并解析响应，从中获得ID值
    return client(requ, requlen);
}

// 向ID服务器发送请求，接收并解析响应，从中获得ID值
long id_c::client(char const* requ, long long requlen) const {
    acl::socket_stream conn;

    // 轮流选择首选实例；连接失败时继续尝试地址表中的其他实例。
    static std::atomic<unsigned int> next_server(0);
    int nids = g_iaddrs.size();
    if (!nids) {
        logger_error("id server address list is empty");
        return -1;
    }
    int nrand = next_server.fetch_add(1, std::memory_order_relaxed) % nids;
    for (int i = 0; i < nids; ++i)
        if (conn.open(g_iaddrs[nrand].c_str(), 0, 0)) {
            logger("connect id success, addr: %s",
                g_iaddrs[nrand].c_str());
            break;
        }
        else {
            logger("connect id fail, addr: %s",
                g_iaddrs[nrand].c_str());
            nrand = (nrand + 1) % nids;
        }
    if (!conn.alive()) {
        logger_error("connect id fail, addrs: %s", cfg_iaddrs);
        return -1;
    }

    // 向ID服务器发送请求
    if (conn.write(requ, requlen) < 0) {
        logger_error("write fail: %s, requlen: %lld, to: %s",
            acl::last_serror(), requlen, conn.get_peer());
        conn.close();
        return -1;
    }

    // 先校验响应头，不能把错误应答的前8字节当作ID。
    char head[HEADLEN];
    if (conn.read(head, HEADLEN) < 0) {
        logger_error("read id response head fail: %s, from: %s",
            acl::last_serror(), conn.get_peer());
        conn.close();
        return -1;
    }

    long long bodylen = ntoll(head);
    int command = (unsigned char)head[BODYLEN_SIZE];
    int status = (unsigned char)head[BODYLEN_SIZE + COMMAND_SIZE];
    if (bodylen != BODYLEN_SIZE || command != CMD_ID_REPLY || status) {
        logger_error("invalid id response, bodylen: %lld, command: %d, "
            "status: %d", bodylen, command, status);
        conn.close();
        return -1;
    }

    char body[BODYLEN_SIZE];
    if (conn.read(body, BODYLEN_SIZE) < 0) {
        logger_error("read id response body fail: %s, from: %s",
            acl::last_serror(), conn.get_peer());
        conn.close();
        return -1;
    }

    // |包体长度|命令|状态|ID值|
    // |    8   |  1 |  1 |  8 |
    long value = ntol(body);
    if (value < 0) {
        logger_error("invalid id value: %ld", value);
        conn.close();
        return -1;
    }

    conn.close();

    return value;
}
