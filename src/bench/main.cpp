#include "showcase.h"
#include "client.h"
#include <fstream>
#include <iostream>
#include <set>
#include <iomanip>
#include <array>
#include <cmath>
#include <atomic>
namespace tnv {
struct Options {std::string sync_dir,run_id,client_id;std::string tracker="127.0.0.1:21010",output;int connections=1,inflight=1,threads=1,users=100,warmup=5,duration=10,timeout=3000;};
// Management-side polling only; the RPC owner reads an atomic flag.
// Declared after Measurement and before clients, so exception unwinding joins
// clients and this monitor before the borrowed flag can be destroyed.
class StopMonitor {
public:
 StopMonitor(const std::string& directory, std::atomic<bool>& requested)
     : path_(std::filesystem::path(directory)/"STOP"), requested_(requested) {
  if(!directory.empty()) thread_=std::thread([this]{watch();});
 }
 ~StopMonitor(){stop();}
 StopMonitor(const StopMonitor&)=delete;
 StopMonitor& operator=(const StopMonitor&)=delete;
 void stop(){
  {std::lock_guard<std::mutex> guard(mutex_);finished_=true;}
  cv_.notify_one();
  if(thread_.joinable()) thread_.join();
 }
 bool failed() const { return failed_.load(); }
private:
 void watch() noexcept {
  try {
   std::unique_lock<std::mutex> lock(mutex_);
   while(!finished_){
    lock.unlock();
    if(std::filesystem::exists(path_)){requested_.store(true);return;}
    lock.lock();
    cv_.wait_for(lock,std::chrono::milliseconds(50),[this]{return finished_;});
   }
  }catch(...){failed_.store(true);requested_.store(true);}
 }
 std::filesystem::path path_;
 std::atomic<bool>& requested_;
 std::atomic<bool> failed_{false};
 std::mutex mutex_;std::condition_variable cv_;bool finished_=false;
 std::thread thread_;
};
struct Counts {uint64_t success=0,business=0,timeout=0,transport=0;};
struct Measurement {
 std::atomic<bool> stop_requested{false};
 std::mutex mutex;std::condition_variable cv;size_t active=0;bool failed=false;
 int64_t actual_start_ns=0,actual_end_ns=0;uint64_t warm_attempt=0,warm_success=0,warm_error=0;
 uint64_t attempt=0,validation=0;Counts window,drain;
 std::vector<uint64_t> histogram=std::vector<uint64_t>(600002,0);
 bool valid(const control::NodesResponse& r){
  if(!valid_nodes(r)||r.group_name()!="group001"||r.nodes_size()!=2)return false;
  std::set<std::string> ids;for(const auto& n:r.nodes()){
   if(n.data_ip()!="127.0.0.1"||n.group_name()!="group001"||!n.active())return false;
   if(!((n.node_id()=="storage-a"&&n.data_port()==23000)||(n.node_id()=="storage-b"&&n.data_port()==23001)))return false;
   ids.insert(n.node_id());
  }return ids.size()==2;
 }
};
struct Slot:std::enable_shared_from_this<Slot> {
 RpcControlClient& client;Measurement& stats;Options& options;size_t connection,user;
 Clock::time_point t0,t1;bool formal;
 Slot(RpcControlClient& c,Measurement& s,Options& o,size_t k,size_t u,Clock::time_point a,Clock::time_point b,bool f):client(c),stats(s),options(o),connection(k),user(u),t0(a),t1(b),formal(f){}
 void finish(){std::lock_guard<std::mutex> g(stats.mutex);--stats.active;stats.cv.notify_all();}
 void submit(){
  if(stats.stop_requested.load()){ {std::lock_guard<std::mutex> g(stats.mutex);stats.failed=true;}finish();return;}
  auto start=Clock::now();if(start>=t1){finish();return;}
  control::NodesRequest q;q.set_appid("tnvideo");std::ostringstream name;name<<"bench"<<std::setfill('0')<<std::setw(6)<<(user++%options.users);q.set_userid(name.str());
  if(formal){std::lock_guard<std::mutex> g(stats.mutex);++stats.attempt;if(!stats.actual_start_ns)stats.actual_start_ns=showcase::now();}else{std::lock_guard<std::mutex> g(stats.mutex);++stats.warm_attempt;}
  try{client.async("GetStorageNodes",q,[self=shared_from_this(),start](Reply r){self->completed(std::move(r),start);},options.timeout,connection);}
  catch(...){ {std::lock_guard<std::mutex> g(stats.mutex);stats.failed=true;if(formal){auto& counts=Clock::now()<t1?stats.window:stats.drain;++counts.transport;}}finish();}
 }
 void completed(Reply r,Clock::time_point start){
  auto end=Clock::now();bool success=r.error==rpc::RpcErrorCode::none&&r.nodes.code()==control::OK;bool validation=success&&!stats.valid(r.nodes);
  {std::lock_guard<std::mutex> g(stats.mutex);
   if(formal){auto& counts=end<t1?stats.window:stats.drain;
    if(validation){++stats.validation;stats.failed=true;}
    else if(r.error==rpc::RpcErrorCode::timeout)++counts.timeout;
    else if(r.error!=rpc::RpcErrorCode::none)++counts.transport;
    else if(r.nodes.code()!=control::OK)++counts.business;
    else{++counts.success;auto us=std::chrono::duration_cast<std::chrono::microseconds>(end-start).count();auto bucket=std::min<uint64_t>(600001,(static_cast<uint64_t>(us)+99)/100);++stats.histogram[bucket];}
   }else{if(success&&!validation)++stats.warm_success;else{++stats.warm_error;stats.failed=true;}}
  }
  // async() only enqueues; the next CallMethod happens after this done returns.
  if(end<t1)submit();else finish();
 }
};
}
int main(int argc,char** argv){using namespace tnv;
 try{
  Options o;
  for(int i=1;i<argc;++i){std::string k=argv[i];if(i+1>=argc)throw std::invalid_argument("missing value");std::string v=argv[++i];
   if(k=="--sync-dir")o.sync_dir=v;else if(k=="--run-id")o.run_id=v;else if(k=="--client-id")o.client_id=v;else if(k=="--tracker")o.tracker=v;else if(k=="--output")o.output=v;else{int n=std::stoi(v);if(k=="--connections")o.connections=n;else if(k=="--inflight")o.inflight=n;else if(k=="--threads")o.threads=n;else if(k=="--users")o.users=n;else if(k=="--warmup")o.warmup=n;else if(k=="--duration")o.duration=n;else if(k=="--timeout-ms")o.timeout=n;else throw std::invalid_argument("unknown option "+k);}}
  auto colon=o.tracker.rfind(':');if(colon==std::string::npos||o.tracker.find_first_not_of("0123456789.:")!=std::string::npos)throw std::invalid_argument("numeric IPv4 tracker required");
  if(o.connections<1||o.connections>256||o.inflight<1||o.inflight>256||o.threads<1||o.threads>o.connections||o.users<1||o.users>100000||o.warmup<0||o.duration<1||o.timeout<1||o.timeout>60000)throw std::invalid_argument("option range");
  Measurement stats;
  StopMonitor monitor(o.sync_dir,stats.stop_requested);
  std::vector<std::unique_ptr<RpcControlClient>> clients;
  for(int i=0;i<o.threads;++i)clients.push_back(std::make_unique<RpcControlClient>(o.tracker,(o.connections-1-i)/o.threads+1,o.inflight));
  for(int i=0;i<o.users;++i){control::NodesRequest q;q.set_appid("tnvideo");std::ostringstream u;u<<"bench"<<std::setfill('0')<<std::setw(6)<<i;q.set_userid(u.str());auto r=clients.front()->call("GetStorageNodes",q,o.timeout);if(r.error!=rpc::RpcErrorCode::none||!stats.valid(r.nodes))throw std::runtime_error("user preparation failed");}
  auto phase=[&](bool formal,Clock::time_point start,Clock::time_point end){
   std::vector<std::shared_ptr<Slot>> slots;
   for(int c=0;c<o.connections;++c)for(int j=0;j<o.inflight;++j)slots.push_back(std::make_shared<Slot>(*clients[c%o.threads],stats,o,c/o.threads,c*o.inflight+j,start,end,formal));
   {std::lock_guard<std::mutex> g(stats.mutex);stats.active=slots.size();}
   std::this_thread::sleep_until(start);
   if(!o.sync_dir.empty()&&Clock::now()-start>std::chrono::milliseconds(100))throw std::runtime_error("late phase start");
   for(auto& s:slots)s->submit();
   std::unique_lock<std::mutex> lock(stats.mutex);
   if(!stats.cv.wait_until(lock,end+std::chrono::milliseconds(o.timeout+5000),[&]{return stats.active==0;})){stats.failed=true;lock.unlock();for(auto& c:clients)c->stop();lock.lock();if(stats.active)throw std::runtime_error("unsettled benchmark slots");}
  };
  auto convert=[](int64_t n){
   auto before=showcase::now();auto steady=std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count();auto after=showcase::now();
   if(steady<before||steady>after)throw std::runtime_error("steady clock is not CLOCK_MONOTONIC");
   return Clock::time_point(std::chrono::duration_cast<Clock::duration>(std::chrono::nanoseconds(n)));
  };
  if(!o.sync_dir.empty()&&(o.run_id.empty()||o.client_id.empty()))throw std::invalid_argument("sync identity required");
  auto warm=Clock::now();auto warm_end=warm+std::chrono::seconds(o.warmup);
  if(!o.sync_dir.empty()){showcase::mark(o.sync_dir,o.run_id,o.client_id,"READY");auto w=showcase::wait(o.sync_dir,o.run_id,"warmup");warm=convert(w.t0);warm_end=convert(w.t1);}
  phase(false,warm,warm_end);
  if(!o.sync_dir.empty())showcase::mark(o.sync_dir,o.run_id,o.client_id,"WARMED");
  if(stats.failed)throw std::runtime_error("warmup failed");
  auto t0=Clock::now()+std::chrono::milliseconds(200),t1=t0+std::chrono::seconds(o.duration);
  int64_t external_t0=0,external_t1=0;
  if(!o.sync_dir.empty()){auto w=showcase::wait(o.sync_dir,o.run_id,"measure");external_t0=w.t0;external_t1=w.t1;o.duration=(w.t1-w.t0)/1000000000;t0=convert(w.t0);t1=t0+std::chrono::nanoseconds(w.t1-w.t0);}
  phase(true,t0,t1);if(!o.sync_dir.empty())showcase::until(external_t1);stats.actual_end_ns=showcase::now();
  for(auto& c:clients)c->stop();
  monitor.stop();
  if(monitor.failed())std::cerr<<"STOP monitor failed; run invalid\n";
  bool normal_close=true;for(auto& c:clients)normal_close=normal_close&&c->clean();
  auto& w=stats.window;auto& d=stats.drain;auto total=w.success+w.business+w.timeout+w.transport+d.success+d.business+d.timeout+d.transport+stats.validation;
  bool valid=normal_close&&!stats.stop_requested.load()&&!stats.failed&&stats.active==0&&total==stats.attempt;
  auto percentile=[&](double fraction){uint64_t n=w.success+d.success;if(!n)return std::string("null");uint64_t rank=static_cast<uint64_t>(std::ceil(n*fraction)),seen=0;for(size_t i=0;i<stats.histogram.size();++i){seen+=stats.histogram[i];if(seen>=rank)return std::to_string(i/10.0);}return std::string("null");};
  std::ostringstream json;
  auto counts=[&](const Counts& c){json<<"{\"success\":"<<c.success<<",\"business_error\":"<<c.business<<",\"timeout\":"<<c.timeout<<",\"transport_or_local_error\":"<<c.transport<<"}";};
  json<<"{\"valid\":"<<(valid?"true":"false")<<",\"load\":\"closed_loop_existing_users_warmed_cache\",\"tracker\":\""<<o.tracker<<"\",\"expected_nodes\":\"storage-a=127.0.0.1:23000,storage-b=127.0.0.1:23001\",\"redis_ttl_seconds\":null,\"connections\":"<<o.connections<<",\"inflight\":"<<o.inflight<<",\"threads\":"<<o.threads<<",\"users\":"<<o.users<<",\"warmup_seconds\":"<<o.warmup<<",\"timeout_ms\":"<<o.timeout<<",\"window_seconds\":"<<std::chrono::duration<double>(t1-t0).count()<<",\"t0_steady_ns\":"<<std::chrono::duration_cast<std::chrono::nanoseconds>(t0.time_since_epoch()).count()<<",\"t1_steady_ns\":"<<std::chrono::duration_cast<std::chrono::nanoseconds>(t1.time_since_epoch()).count()<<",\"attempt\":"<<stats.attempt<<",\"window\":";counts(w);json<<",\"drain\":";counts(d);json<<",\"validation_error\":"<<stats.validation<<",\"pending_final\":"<<stats.active<<",\"success_qps\":"<<w.success/double(o.duration)<<",\"p50_ms\":"<<percentile(.5)<<",\"p95_ms\":"<<percentile(.95)<<",\"p99_ms\":"<<percentile(.99)<<",\"histogram_resolution_ms\":0.1,\"histogram_overflow_above_ms\":60000,\"latency_includes_drain\":true,\"normal_close\":"<<(normal_close?"true":"false")<<"}";
  if(!o.sync_dir.empty()){auto text=json.str();text.pop_back();json.str("");json.clear();json<<text<<",\"stop_requested\":"<<(stats.stop_requested.load()?"true":"false")<<",\"stop_monitor_error\":"<<(monitor.failed()?"true":"false")<<",\"clock\":\"CLOCK_MONOTONIC\",\"t0_ns\":"<<external_t0<<",\"t1_ns\":"<<external_t1<<",\"actual_start_ns\":"<<stats.actual_start_ns<<",\"actual_end_ns\":"<<stats.actual_end_ns<<",\"warmup_attempt\":"<<stats.warm_attempt<<",\"warmup_success\":"<<stats.warm_success<<",\"warmup_error\":"<<stats.warm_error<<",\"histogram\":"<<showcase::histogram(stats.histogram)<<"}";}
  std::cout<<json.str()<<'\n';if(!o.output.empty()){std::ofstream out(o.output);out<<json.str()<<'\n';if(!out)throw std::runtime_error("output write failed");}
  return valid?0:1;
 }catch(const std::exception& e){std::cerr<<"tnv_bench: "<<e.what()<<'\n';return 1;}
}
