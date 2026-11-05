#include "showcase.h"
#include "07_client.h"
#include "01_types.h"
#include <openssl/sha.h>
#include <atomic>
#include <cctype>
#include <iostream>
#include <iomanip>
#include <map>
#include <mutex>
#include <sys/statvfs.h>
#include <unistd.h>
using namespace showcase;
constexpr uint64_t bytes=16*1024*1024;
std::string digest(const fs::path& p){std::ifstream f(p,std::ios::binary);SHA256_CTX c;SHA256_Init(&c);char b[65536];uint64_t size=0;while(f){f.read(b,sizeof b);auto n=f.gcount();size+=n;SHA256_Update(&c,b,n);}if(size!=bytes)throw std::runtime_error("source/target size");unsigned char d[32];SHA256_Final(d,&c);std::ostringstream o;for(auto v:d)o<<std::hex<<std::setw(2)<<std::setfill('0')<<unsigned(v);return o.str();}
void check_disk_space(const fs::path& p){struct statvfs v{};if(statvfs(p.c_str(),&v)||uint64_t(v.f_bavail)*v.f_frsize<1024ULL*1024*1024)throw std::runtime_error("disk below 1GiB");}
void clean(const fs::path& p){for(auto suffix:{"",".part",".part.meta",".part.meta.tmp",".part.lock"}){auto f=fs::path(p.string()+suffix);if(fs::is_symlink(fs::symlink_status(f)))throw std::runtime_error("cleanup symlink refused");fs::remove(f);}}
struct Seed{std::string user,id,sha;};
int main(int argc,char** argv){
 std::map<std::string,std::string> a;
 try{
  for(int i=1;i<argc;i+=2){if(i+1==argc)throw std::runtime_error("missing argument");a[argv[i]]=argv[i+1];}
  for(auto key:{"--mode","--dir","--run-id"})if(a[key].empty())throw std::runtime_error("required mode/dir/run-id");
  std::string mode=a["--mode"],run=a["--run-id"],cid=a["--client-id"],tracker=a.count("--tracker")?a["--tracker"]:"127.0.0.1:21010";
  for(char ch:run+cid)if(!isalnum(static_cast<unsigned char>(ch)))throw std::runtime_error("ASCII alphanumeric run/client identity required");
  if(run.size()>48||cid.size()>8)throw std::runtime_error("identity too long");
  fs::path dir=fs::absolute(a["--dir"]);fs::create_directories(dir);if(dir.string().find("/mnt/hgfs")==0)throw std::runtime_error("local disk required");check_disk_space(dir);check_disk_space("/mnt/tnv/group001");
  if(client_c::init(tracker.c_str())!=OK)throw std::runtime_error("SDK init");
  auto source=dir/"source.bin";
  if(mode=="prepare"){
   if(fs::exists(dir/"seeds.tsv")||fs::exists(source))throw std::runtime_error("existing seed/source refused");
   {std::ifstream random("/dev/urandom",std::ios::binary);std::ofstream out(source,std::ios::binary);char b[65536];for(uint64_t n=0;n<bytes;n+=sizeof b){random.read(b,sizeof b);out.write(b,sizeof b);}if(!random||!out)throw std::runtime_error("source generation");}
   auto sha=digest(source);std::ofstream manifest(dir/"seeds.tsv");client_c c;
   for(int i=0;i<8;++i){auto user="seed"+run;auto id=user+std::to_string(i);manifest<<user<<' '<<id<<' '<<sha<<'\n';manifest.flush();if(c.upload("tnvideo",user.c_str(),id.c_str(),source.c_str())!=OK)throw std::runtime_error("seed upload failed; manifest retained");}
   const bool normal_close=client_c::deinit_checked();
   if(!normal_close)std::cerr<<"SDK close failed after prepare\n";
   return normal_close?0:1;
  }
  std::vector<Seed> seeds;{std::ifstream m(dir/"seeds.tsv");Seed s;while(m>>s.user>>s.id>>s.sha)seeds.push_back(s);}
  if(seeds.size()!=8)throw std::runtime_error("eight seed manifest entries required");
  if(mode=="cleanup"){client_c c;bool ok=true;for(auto& s:seeds)ok=(c.del("tnvideo",s.user.c_str(),s.id.c_str())==OK)&&ok;const bool normal_close=client_c::deinit_checked();if(!normal_close)std::cerr<<"SDK close failed after cleanup\n";return ok&&normal_close?0:1;}
  if(mode!="upload"&&mode!="download")throw std::runtime_error("mode");
  int workers=std::stoi(a["--workers"]);if(workers<1||workers>2)throw std::runtime_error("workers 1..2");
  fs::path sync=a["--sync-dir"],local=sync/("files"+cid);if(!fs::create_directory(local))throw std::runtime_error("local directory exists");
  auto sha=digest(source);
  {client_c c;for(const auto& seed:seeds){long long n=0;std::string d;if(c.filemeta("tnvideo",seed.user.c_str(),seed.id.c_str(),&n,d)!=OK||n!=bytes||d!=seed.sha)throw std::runtime_error("seed metadata differs from manifest");}}
  int64_t actual_start_ns=0,actual_end_ns=0;
  std::atomic<bool> failed{false};std::mutex mutex;uint64_t attempt=0,ws=0,ds=0,error=0,cleanup_error=0,retries=0;std::map<std::string,uint64_t> payload;
  std::vector<uint64_t> h(1200002,0);std::vector<std::thread> threads;
  // Per-process preparation proves 16MiB multi-block upload/download/checksum/delete.
  {client_c c;c.set_preferred_node(a["--preferred"]);auto user="prep"+run+cid,id=user;std::ofstream log(local/"prepare.tsv");log<<user<<' '<<id<<" uploading\n";log.flush();if(c.upload("tnvideo",user.c_str(),id.c_str(),source.c_str())!=OK)throw std::runtime_error("prepare upload");auto target=local/"prepare.bin";if(c.download_to_file("tnvideo",user.c_str(),id.c_str(),target.c_str())!=OK||digest(target)!=sha)throw std::runtime_error("prepare download/checksum");if(c.del("tnvideo",user.c_str(),id.c_str())!=OK)throw std::runtime_error("prepare delete");clean(target);log<<user<<' '<<id<<" deleted\n";}
  mark(sync,run,cid,"READY");auto warm=wait(sync,run,"warmup");
  auto phase=[&](Window w,bool formal){
   until(w.t0);if(now()-w.t0>100000000)throw std::runtime_error("late start");
   for(int worker=0;worker<workers;++worker)threads.emplace_back([&,worker]{
    client_c c;c.set_preferred_node(a["--preferred"]);uint64_t seq=0;std::ofstream journal(local/(std::to_string(worker)+(formal?"formal.tsv":"warm.tsv")));
    try{while(now()<w.t1&&!failed&&!fs::exists(sync/"STOP")){
     check_disk_space(dir);check_disk_space("/mnt/tnv/group001");std::string user="file"+run+cid+std::to_string(worker);std::string id=user+(formal?"f":"w")+std::to_string(seq++);Seed seed=seeds[(worker+seq)%seeds.size()];auto target=local/(id+".bin");
     journal<<user<<' '<<id<<" prepared\n"<<user<<' '<<id<<" uploading\n";journal.flush();c.reset_transfer_stats();auto start=now();if(start>=w.t1)break;
     if(formal){std::lock_guard<std::mutex> g(mutex);++attempt;if(!actual_start_ns)actual_start_ns=start;}
     int result=mode=="upload"?c.upload("tnvideo",user.c_str(),id.c_str(),source.c_str()):c.download_to_file("tnvideo",seed.user.c_str(),seed.id.c_str(),target.c_str());auto end=now();
     {std::lock_guard<std::mutex> g(mutex);if(formal){for(auto& p:c.transfer_stats().payload)payload[p.first]+=p.second;retries+=c.transfer_stats().retry_events;if(result==OK){++(end<w.t1?ws:ds);++h[std::min<uint64_t>(1200001,(end-start+99999)/100000)];}else{++error;failed=true;}}else if(result!=OK)failed=true;}
     journal<<user<<' '<<id<<(result==OK?" committed\n":" failed\n");journal.flush();if(result!=OK)break;
     bool cleaned=true;if(mode=="upload")cleaned=c.del("tnvideo",user.c_str(),id.c_str())==OK;else clean(target);
     journal<<user<<' '<<id<<(cleaned?" deleted\n":" cleanup_failed\n");journal.flush();if(!cleaned){std::lock_guard<std::mutex> g(mutex);++cleanup_error;failed=true;break;}
    }}catch(const std::exception& e){std::cerr<<e.what()<<'\n';failed=true;}
   });
   for(auto& t:threads) {
    t.join();
   }
   threads.clear();
  };
  phase(warm,false);mark(sync,run,cid,"WARMED");auto w=wait(sync,run,"measure");phase(w,true);until(w.t1);actual_end_ns=now();
  // Both phases have joined every file worker; only the management thread closes SDK.
  const bool normal_close=client_c::deinit_checked();
  if(!normal_close)std::cerr<<"SDK close failed; file run invalid\n";
  auto unfinished=attempt-ws-ds-error;bool valid=normal_close&&!failed&&!cleanup_error&&!unfinished&&!fs::exists(sync/"STOP");
  std::ostringstream out;out<<"{\"valid\":"<<(valid?"true":"false")<<",\"normal_close\":"<<(normal_close?"true":"false")<<",\"t0_ns\":"<<w.t0<<",\"t1_ns\":"<<w.t1<<",\"actual_start_ns\":"<<actual_start_ns<<",\"actual_end_ns\":"<<actual_end_ns<<",\"attempt\":"<<attempt<<",\"window\":{\"success\":"<<ws<<"},\"drain\":{\"success\":"<<ds<<"},\"api_error\":"<<error<<",\"validation_error\":0,\"unfinished\":"<<unfinished<<",\"cleanup_error\":"<<cleanup_error<<",\"success_bytes\":"<<ws*bytes<<",\"retry_events\":"<<retries<<",\"histogram_resolution_ms\":0.1,\"histogram_overflow_above_ms\":120000,\"histogram\":"<<histogram(h)<<",\"payload\":{";bool first=true;for(auto& p:payload){if(!first)out<<',';first=false;out<<'\"'<<p.first<<"\":"<<p.second;}out<<"}}";atomic(a["--output"],out.str());return valid?0:1;
 }catch(const std::exception& e){std::cerr<<"file bench: "<<e.what()<<'\n';return 1;}
}
