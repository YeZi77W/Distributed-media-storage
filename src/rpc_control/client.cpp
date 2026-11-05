#include "client.h"
#include "highperfrpc/net/inet_address.h"
#include <sstream>
#include <poll.h>
#include <iostream>
namespace tnv {
namespace {
struct Call:google::protobuf::Closure {
 rpc::RpcController controller; std::unique_ptr<google::protobuf::Message> response;
 RpcControlClient::Callback cb;
 void Run() override {
  Reply r; r.error=controller.error_code(); r.text=controller.ErrorText();
  if(auto* p=dynamic_cast<control::Status*>(response.get())) r.status=*p;
  if(auto* p=dynamic_cast<control::NodesResponse*>(response.get())) r.nodes=*p;
  if(auto* p=dynamic_cast<control::GroupsResponse*>(response.get())) r.groups=*p;
  cb(std::move(r)); delete this;
 }
};
struct Session { std::unique_ptr<control::TrackerControlService::Stub> stub; std::shared_ptr<rpc::Connector> connector; std::shared_ptr<rpc::RpcChannel> channel; rpc::TimerId timer; bool ready=false; };
}
RpcControlClient::RpcControlClient(std::string a,size_t n,size_t k):address_(std::move(a)),count_(n),inflight_(k) {
 if(!n||!k) throw std::invalid_argument("client capacity");
 thread_=std::thread([this]{run();});
}
RpcControlClient::~RpcControlClient(){stop();}
void RpcControlClient::stop(){ {std::lock_guard<std::mutex> g(mutex_);stopping_=true;} notify(wake_.fd); if(thread_.joinable()) thread_.join(); }
void RpcControlClient::async(std::string method,const google::protobuf::Message& req,Callback cb,int ms,size_t index){
 auto j=std::make_shared<Job>(); j->method=std::move(method); j->request.reset(req.New()); j->request->CopyFrom(req); j->cb=std::move(cb);j->deadline=Clock::now()+std::chrono::milliseconds(ms);j->connection=index;
 {std::lock_guard<std::mutex> g(mutex_); if(stopping_||queue_.size()>=65536) throw std::runtime_error("client stopped/full");queue_.push_back(j);} notify(wake_.fd);
}
Reply RpcControlClient::call(std::string m,const google::protobuf::Message& req,int ms){
 if(thread_.get_id()==std::this_thread::get_id()) throw std::logic_error("synchronous call on IO owner");
 auto p=std::make_shared<std::promise<Reply>>();auto f=p->get_future();async(std::move(m),req,[p](Reply r){p->set_value(std::move(r));},ms);
 if(f.wait_for(std::chrono::milliseconds(ms))!=std::future_status::ready) {Reply r;r.error=rpc::RpcErrorCode::timeout;r.text="end-to-end deadline";return r;} return f.get();
}
void RpcControlClient::run(){
 try {
  owner_=std::this_thread::get_id();
  auto colon=address_.rfind(':'); if(colon==std::string::npos) throw std::invalid_argument("RPC address");
  auto ip=address_.substr(0,colon);int port=std::stoi(address_.substr(colon+1));if(port<1||port>65535) throw std::invalid_argument("RPC port");
  for(;;){
   {std::lock_guard<std::mutex> g(mutex_);if(stopping_) break;}
   rpc::EventLoop loop;rpc::Channel wake(wake_.fd);std::vector<Session> sessions(count_); bool closing=false;
   auto close=[&]{ if(closing)return;closing=true;for(auto& s:sessions){if(s.connector)s.connector->cancel();if(s.channel)s.channel->close();}loop.quit();};
   std::function<void()> pump;
   pump=[&]{
    bool stop;{std::lock_guard<std::mutex> g(mutex_);stop=stopping_;}if(stop){close();return;}
    std::deque<std::shared_ptr<Job>> jobs;{std::lock_guard<std::mutex> g(mutex_);jobs.swap(queue_);}
    for(auto& j:jobs){
     auto& s=sessions[j->connection%count_];
     if(Clock::now()>=j->deadline){Reply r;r.error=rpc::RpcErrorCode::timeout;j->cb(std::move(r));continue;}
     if(!s.ready){std::lock_guard<std::mutex> g(mutex_);queue_.push_back(j);continue;}
     if(!s.channel||s.channel->state()!=rpc::RpcChannelState::Open){Reply r;r.error=rpc::RpcErrorCode::not_connected;j->cb(std::move(r));continue;}
     const auto* method=control::TrackerControlService::descriptor()->FindMethodByName(j->method);
     if(!method){Reply r;r.error=rpc::RpcErrorCode::bad_request;j->cb(std::move(r));continue;}
     auto* c=new Call; c->cb=std::move(j->cb); c->controller.set_deadline(j->deadline);
     if(j->method=="GetStorageNodes")c->response=std::make_unique<control::NodesResponse>();
     else if(j->method=="ListGroups")c->response=std::make_unique<control::GroupsResponse>();
     else c->response=std::make_unique<control::Status>();
     if(j->method=="GetStorageNodes")s.stub->GetStorageNodes(&c->controller,static_cast<control::NodesRequest*>(j->request.get()),static_cast<control::NodesResponse*>(c->response.get()),c);
     else if(j->method=="ListGroups")s.stub->ListGroups(&c->controller,static_cast<control::GroupsRequest*>(j->request.get()),static_cast<control::GroupsResponse*>(c->response.get()),c);
     else if(j->method=="RegisterStorage")s.stub->RegisterStorage(&c->controller,static_cast<control::RegisterRequest*>(j->request.get()),static_cast<control::Status*>(c->response.get()),c);
     else s.stub->Heartbeat(&c->controller,static_cast<control::HeartbeatRequest*>(j->request.get()),static_cast<control::Status*>(c->response.get()),c);
    }
   };
   wake.set_read_callback([&]{consume(wake_.fd);pump();});wake.enable_reading();loop.update_channel(wake);
   try {
    for(auto& s:sessions){auto* ptr=&s;
     s.connector=rpc::Connector::create(loop,rpc::InetAddress(ip,static_cast<uint16_t>(port)),[&,ptr](rpc::ConnectorResult r){
      loop.cancel_timer(ptr->timer);ptr->ready=true;
      if(r.status!=rpc::ConnectorStatus::connected){pump();close();return;}
      ptr->channel=rpc::RpcChannel::create(loop,std::move(r.socket),{},rpc::RpcChannelConfig{inflight_});
      ptr->stub=std::make_unique<control::TrackerControlService::Stub>(ptr->channel.get());
      ptr->channel->set_close_ready_callback([&]{close();});ptr->channel->start();pump();
     });
     auto t=loop.run_at(Clock::now()+std::chrono::seconds(3),[&,ptr]{ptr->connector->cancel();close();});
     if(t.status!=rpc::TimerAddStatus::added)throw std::runtime_error("connect timer");s.timer=t.id;s.connector->start();
    }
    // Queue expiration includes time spent waiting for establishment. Not a completion poll.
    std::function<void()> expire;expire=[&]{if(!closing){pump();timer(loop,Clock::now()+std::chrono::milliseconds(50),expire);}};
    timer(loop,Clock::now()+std::chrono::milliseconds(50),expire);
    loop.loop();
   }catch(const std::exception& e){clean_=false;std::cerr<<"RPC client: "<<e.what()<<'\n';close();}
   // Idle stack, after all Loop callbacks returned.
   for(auto& s:sessions)if(s.connector)s.connector->cancel();loop.drain_deferred();
   for(auto& s:sessions)if(s.channel){s.channel->close();s.channel->finish_close();if(s.channel->state()!=rpc::RpcChannelState::Closed||s.channel->pending_count()||s.channel->last_exception())clean_=false;}
   loop.drain_deferred();for(auto& s:sessions)if(s.connector&&s.connector->state()!=rpc::ConnectorState::Finished)clean_=false;wake.disable_all();loop.remove_channel(wake);
   std::deque<std::shared_ptr<Job>> failed;{std::lock_guard<std::mutex> g(mutex_);failed.swap(queue_);}
   for(auto& j:failed){Reply r;r.error=rpc::RpcErrorCode::not_connected;j->cb(std::move(r));}
   std::unique_lock<std::mutex> lock(mutex_);if(stopping_)break;lock.unlock();
   // Interruptible reconnection wait; wake fd also wakes stop immediately.
   pollfd fd{wake_.fd,POLLIN,0};::poll(&fd,1,2000);
  }
 }catch(const std::exception& e){clean_=false;std::cerr<<"RPC owner: "<<e.what()<<'\n';}
 std::deque<std::shared_ptr<Job>> failed;{std::lock_guard<std::mutex> g(mutex_);stopping_=true;failed.swap(queue_);}
 for(auto& j:failed){Reply r;r.error=rpc::RpcErrorCode::not_connected;j->cb(std::move(r));}
}
bool valid_nodes(const control::NodesResponse& r){if(r.code()!=control::OK||r.group_name().empty()||r.nodes().empty())return false;for(const auto& n:r.nodes())if(n.node_id().empty()||n.group_name()!=r.group_name()||n.data_ip().empty()||!n.data_port()||n.data_port()>65535)return false;return true;}
std::string format_groups(const control::GroupsResponse& r){std::ostringstream o; o<<"COUNT OF GROUPS: "<<r.groups_size()<<'\n';for(const auto& g:r.groups()){int active=0;for(const auto& n:g.nodes())active+=n.active();o<<"GROUPNAME: "<<g.group_name()<<"\nCOUNT OF ACTIVE STORAGES: "<<active<<'\n';for(const auto& n:g.nodes())o<<n.node_id()<<' '<<n.data_ip()<<':'<<n.data_port()<<" active="<<n.active()<<'\n';}return o.str();}
}
