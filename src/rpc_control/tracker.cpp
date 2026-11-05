#include "client.h"
#include "../02_tracker/01_globals.h"
#include "../02_tracker/03_cache.h"
#include "../02_tracker/05_db.h"
#include "03_util.h"
#include "highperfrpc/rpc/rpc_server.h"
#include "highperfrpc/rpc/service_registry.h"
#include "highperfrpc/net/inet_address.h"
#include <sys/signalfd.h>
#include <arpa/inet.h>
#include <signal.h>
#include <iostream>
#include <map>
#include <algorithm>
#include "02_proto.h"
#include <set>
namespace tnv {
struct Route { uint64_t id; std::string app,user,group,error; std::atomic<bool> ready{false}; };
class BusinessPool {
public:
 Wake wake; std::mutex mutex; std::condition_variable cv;bool stopping=false;
 std::deque<std::shared_ptr<Route>> todo;std::vector<std::thread> threads;
 explicit BusinessPool(int n){try{for(int i=0;i<n;++i)threads.emplace_back([this]{work();});}catch(...){stop();join();throw;}}
 ~BusinessPool(){stop();join();}
 void stop(){{std::lock_guard<std::mutex> g(mutex);stopping=true;}cv.notify_all();}
 void join(){for(auto& t:threads)if(t.joinable())t.join();}
 void work(){
  mysql_thread_init();std::unique_ptr<db_c> db;
  for(;;){std::shared_ptr<Route> job;{std::unique_lock<std::mutex> g(mutex);cv.wait(g,[&]{return stopping||!todo.empty();});if(todo.empty())break;job=std::move(todo.front());todo.pop_front();}
   Route& r=*job;
   try {
    cache_c cache;acl::string key,value;key.format("route_v2:%zu:%s:%s",r.app.size(),r.app.c_str(),r.user.c_str());
    if(cache.get(key,value)==OK)r.group=value.c_str();
    else {
     if(!db){db=std::make_unique<db_c>();if(db->connect()!=OK)throw std::runtime_error("route connect failed");}
     if(db->get(r.app.c_str(),r.user.c_str(),r.group)!=OK)throw std::runtime_error("route query failed");
     if(r.group.empty()){std::vector<std::string> groups;if(db->get(groups)!=OK||groups.empty())throw std::runtime_error("route groups unavailable");r.group=groups[std::hash<std::string>{}(r.app+r.user)%groups.size()];if(db->set(r.app.c_str(),r.user.c_str(),r.group.c_str())!=OK)throw std::runtime_error("route insert failed");if(db->get(r.app.c_str(),r.user.c_str(),r.group)!=OK||r.group.empty())throw std::runtime_error("route winner missing");}
     cache.set(key,r.group.c_str());
    }
   }catch(const std::exception& e){r.error=e.what();db.reset();}catch(...){r.error="worker exception";db.reset();}
   r.ready.store(true,std::memory_order_release);notify(wake.fd);
  }db.reset();mysql_thread_end();
 }
};
struct NodeState { control::Node node; Clock::time_point beat; };
class Tracker final:public control::TrackerControlService {
public:
 BusinessPool& pool;size_t capacity;int offline;bool accepting=true;uint64_t sequence=0;
 std::map<std::string,NodeState> nodes;
 struct Borrow {control::NodesResponse* response;google::protobuf::Closure* done;std::shared_ptr<Route> route;};
 std::map<uint64_t,Borrow> pending;
 Tracker(BusinessPool& p,size_t cap,int seconds):pool(p),capacity(cap),offline(seconds){}
 bool active(NodeState& n){bool a=Clock::now()-n.beat<std::chrono::seconds(offline);if(n.node.active()!=a)std::cerr<<"node "<<n.node.node_id()<<" active="<<a<<'\n';n.node.set_active(a);return a;}
 void scan(){for(auto& p:nodes)active(p.second);}
 void RegisterStorage(google::protobuf::RpcController*,const control::RegisterRequest* q,control::Status* r,google::protobuf::Closure* d) override {
  in_addr ip{};
  if(!accepting){r->set_code(control::BUSY);d->Run();return;}
  if(q->node_id().empty()||q->node_id().size()>128||q->group_name().empty()||q->group_name().size()>STORAGE_GROUPNAME_MAX||q->version().size()>STORAGE_VERSION_MAX||inet_pton(AF_INET,q->data_ip().c_str(),&ip)!=1||!q->data_port()||q->data_port()>65535){r->set_code(control::INVALID_ARGUMENT);d->Run();return;}
  NodeState n;auto& v=n.node;v.set_node_id(q->node_id());v.set_group_name(q->group_name());v.set_data_ip(q->data_ip());v.set_data_port(q->data_port());v.set_version(q->version());v.set_startup_unix_seconds(q->startup_unix_seconds());v.set_active(true);n.beat=Clock::now();nodes[q->node_id()]=std::move(n);std::cerr<<"registered "<<q->node_id()<<'\n';d->Run();
 }
 void Heartbeat(google::protobuf::RpcController*,const control::HeartbeatRequest* q,control::Status* r,google::protobuf::Closure* d) override {
  auto it=nodes.find(q->node_id());if(!accepting)r->set_code(control::BUSY);else if(it==nodes.end()||it->second.node.group_name()!=q->group_name())r->set_code(control::NOT_FOUND);else{it->second.beat=Clock::now();it->second.node.set_active(true);}d->Run();
 }
 void ListGroups(google::protobuf::RpcController*,const control::GroupsRequest*,control::GroupsResponse* r,google::protobuf::Closure* d) override {
  scan();std::map<std::string,control::Group*> groups;for(auto& p:nodes){auto name=p.second.node.group_name();if(!groups.count(name)){auto* g=r->add_groups();g->set_group_name(name);groups[name]=g;}*groups[name]->add_nodes()=p.second.node;}d->Run();
 }
 void GetStorageNodes(google::protobuf::RpcController*,const control::NodesRequest* q,control::NodesResponse* r,google::protobuf::Closure* d) override {
  if(!accepting||pending.size()>=capacity){r->set_code(control::BUSY);d->Run();return;}
  if(q->appid().empty()||q->appid().size()>=APPID_SIZE||q->userid().empty()||q->userid().size()>=USERID_SIZE||valid(q->appid().c_str())!=OK||valid(q->userid().c_str())!=OK||std::find(g_appids.begin(),g_appids.end(),q->appid())==g_appids.end()){r->set_code(control::INVALID_ARGUMENT);d->Run();return;}
  auto id=++sequence;auto route=std::make_shared<Route>();route->id=id;route->app=q->appid();route->user=q->userid();pending.emplace(id,Borrow{r,d,route});
  try{{std::lock_guard<std::mutex> g(pool.mutex);pool.todo.push_back(route);}pool.cv.notify_one();}catch(...){pending.erase(id);r->set_code(control::INTERNAL_ERROR);d->Run();}
 }
 void complete(){
  consume(pool.wake.fd);
  for(auto it=pending.begin();it!=pending.end();){
   if(!it->second.route->ready.load(std::memory_order_acquire)){++it;continue;}
   auto b=it->second;it=pending.erase(it);auto& result=*b.route;
   auto* r=b.response;
   try {
    if(!result.error.empty()){r->set_code(control::INTERNAL_ERROR);r->set_message(result.error);}
    else{r->set_group_name(result.group);for(auto& p:nodes)if(p.second.node.group_name()==result.group&&active(p.second)){*r->add_nodes()=p.second.node;if(r->nodes_size()==3)break;}if(r->nodes().empty())r->set_code(control::NO_ACTIVE_STORAGE);}
   }catch(...){r->Clear();r->set_code(control::INTERNAL_ERROR);}
   b.done->Run();
  }
 }

};
}
int main(int argc,char** argv){using namespace tnv;
 try{
  sigset_t signals;sigemptyset(&signals);sigaddset(&signals,SIGINT);sigaddset(&signals,SIGTERM);if(pthread_sigmask(SIG_BLOCK,&signals,nullptr))throw std::runtime_error("signal mask");
  std::string config="../etc/tracker.cfg",listen="127.0.0.1:21010";
  for(int i=1;i<argc;++i){std::string k=argv[i];if(k=="--config"&&i+1<argc)config=argv[++i];else if(k=="--listen"&&i+1<argc)listen=argv[++i];else throw std::invalid_argument("tracker --config PATH --listen IP:PORT");}
  if(access(config.c_str(),R_OK))throw std::runtime_error("unreadable tracker config");
  acl::acl_cpp_init(); // Keep default log verbosity; routing cache log calls removed below.
  acl::master_conf conf;conf.set_cfg_str(cfg_str);conf.set_cfg_int(cfg_int);conf.load(config.c_str());
  split(cfg_appids,g_appids);split(cfg_maddrs,g_maddrs);split(cfg_raddrs,g_raddrs);
  if(!g_raddrs.empty()){g_rconns=new acl::redis_client_pool(g_raddrs.front().c_str(),cfg_maxconns);g_rconns->set_timeout(cfg_ctimeout,cfg_rtimeout);}
  int workers=4,capacity=1024,offline=12;acl::master_int_tbl settings[]={{"tnv_rpc_workers",4,&workers,1,64},{"tnv_rpc_capacity",1024,&capacity,1,1024},{"tnv_offline_seconds",12,&offline,1,3600},{0,0,0,0,0}};
  acl::master_conf extra;extra.set_cfg_int(settings);extra.load(config.c_str());
  auto pos=listen.rfind(':');if(pos==std::string::npos)throw std::invalid_argument("tracker address");int port=std::stoi(listen.substr(pos+1));if(port<1||port>65535)throw std::invalid_argument("tracker port");
  rpc::EventLoop loop;BusinessPool pool(workers);auto service=std::make_shared<Tracker>(pool,capacity,offline);auto registry=std::make_shared<rpc::ServiceRegistry>();registry->register_service(service);registry->freeze();
  rpc::RpcServerConfig options;options.transport.sub_io_threads=0;options.max_calls=2048;options.max_calls_per_connection=256;
  rpc::RpcServer server(loop,rpc::InetAddress(listen.substr(0,pos),static_cast<uint16_t>(port)),registry,options);
  int sigfd=signalfd(-1,&signals,SFD_CLOEXEC|SFD_NONBLOCK);if(sigfd<0)throw std::runtime_error("signalfd");
  rpc::Channel completed(pool.wake.fd),signal(sigfd);
  completed.set_read_callback([&]{service->complete();});completed.enable_reading();loop.update_channel(completed);
  auto stop=[&]{service->accepting=false;pool.stop();server.stop();};
  signal.set_read_callback([&]{signalfd_siginfo info;while(read(sigfd,&info,sizeof(info))<0&&errno==EINTR){}stop();});signal.enable_reading();loop.update_channel(signal);
  server.set_join_ready_callback([&]{loop.quit();});
  std::function<void()> scan;scan=[&]{service->scan();if(service->accepting)timer(loop,Clock::now()+std::chrono::seconds(1),scan);};
  try{server.start();timer(loop,Clock::now()+std::chrono::seconds(1),scan);loop.loop();}catch(const std::exception& e){std::cerr<<"Tracker Loop failed: "<<e.what()<<'\n';stop();if(server.snapshot().state!=rpc::RpcServerState::JoinReady)loop.loop();}
  if(server.finish_stop()!=rpc::RpcServerFinishResult::stopped)throw std::runtime_error("pending RPC at finish_stop");
  pool.stop();pool.join();if(!service->pending.empty())throw std::runtime_error("pending business");
  completed.disable_all();loop.remove_channel(completed);signal.disable_all();loop.remove_channel(signal);close(sigfd);delete g_rconns;g_rconns=nullptr;
  return 0;
 }catch(const std::exception& e){std::cerr<<"tracker: "<<e.what()<<'\n';return 1;}
}
