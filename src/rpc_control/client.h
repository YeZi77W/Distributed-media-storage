#pragma once
#include "tnv/control.pb.h"
#include "highperfrpc/net/event_loop.h"
#include "highperfrpc/net/channel.h"
#include "highperfrpc/net/connector.h"
#include "highperfrpc/rpc/rpc_channel.h"
#include <sys/eventfd.h>
#include <unistd.h>
#include <cerrno>
#include <stdexcept>
#include <mutex>
#include <thread>
#include <condition_variable>
#include <deque>
#include <future>
#include <atomic>
namespace tnv {
namespace rpc = highperfrpc;
using Clock = rpc::TimerClock;
inline void notify(int fd) { uint64_t n=1; while (::write(fd,&n,8)<0) { if(errno==EINTR) continue; if(errno==EAGAIN) return; throw std::runtime_error("eventfd write"); } }
inline void consume(int fd) { uint64_t n; while (::read(fd,&n,8)<0) { if(errno==EINTR) continue; if(errno==EAGAIN) return; throw std::runtime_error("eventfd read"); } }
struct Wake {
 int fd=::eventfd(0,EFD_CLOEXEC|EFD_NONBLOCK);
 Wake() { if(fd<0) throw std::runtime_error("eventfd"); }
 ~Wake(){::close(fd);} Wake(const Wake&)=delete;
};
inline void timer(rpc::EventLoop& l, Clock::time_point t, std::function<void()> f) {
 if(l.run_at(t,std::move(f)).status!=rpc::TimerAddStatus::added) throw std::runtime_error("timer rejected");
}
struct Reply { rpc::RpcErrorCode error=rpc::RpcErrorCode::none; std::string text; control::Status status; control::NodesResponse nodes; control::GroupsResponse groups; };
// Thread-safe queue, absolute end-to-end deadlines. Each session belongs to its IO owner.
class RpcControlClient {
public:
 using Callback=std::function<void(Reply)>;
 explicit RpcControlClient(std::string address, size_t connections=1, size_t inflight=256);
 ~RpcControlClient();
 RpcControlClient(const RpcControlClient&)=delete;
 void async(std::string method, const google::protobuf::Message& request, Callback cb, int timeout_ms=3000, size_t connection=0);
 Reply call(std::string method,const google::protobuf::Message& request,int timeout_ms=3000);
 void stop();
 bool clean() const { return clean_.load(); }
private:
 struct Job { std::string method; std::unique_ptr<google::protobuf::Message> request; Callback cb; Clock::time_point deadline; size_t connection; };
 void run();
 std::string address_; size_t count_, inflight_; Wake wake_;
 std::mutex mutex_; std::deque<std::shared_ptr<Job>> queue_; bool stopping_=false;
 std::thread thread_; std::thread::id owner_;
 std::atomic<bool> clean_{true};
};
std::string format_groups(const control::GroupsResponse&);
bool valid_nodes(const control::NodesResponse&);
}
