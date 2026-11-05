// 存储服务器
// 声明跟踪客户机线程类
//
#pragma once

#include <lib_acl.hpp>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include "client.h"
//
// 跟踪客户机线程类
//
class tracker_c: public acl::thread {
public:
    // 构造函数
    tracker_c(char const* taddr);

    // 终止线程
    void stop(void);

protected:
    // 线程过程
    void* run(void);

private:
    std::atomic<bool> m_stop;
    acl::string m_taddr;
    std::mutex m_mutex;
    std::condition_variable m_cv;
    std::unique_ptr<tnv::RpcControlClient> m_client;
};
