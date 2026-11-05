#pragma once
#include <filesystem>
#include <algorithm>
#include <cstdint>
#include <fstream>
#include <sstream>
#include <thread>
#include <chrono>
#include <stdexcept>
#include <vector>
#include <ctime>
namespace showcase {
namespace fs=std::filesystem;
inline int64_t now(){timespec t{};if(clock_gettime(CLOCK_MONOTONIC,&t))throw std::runtime_error("clock_gettime");return int64_t(t.tv_sec)*1000000000+t.tv_nsec;}
inline void atomic(const fs::path& path,const std::string& text){auto tmp=path;tmp+=".tmp";{std::ofstream f(tmp);f<<text;if(!f)throw std::runtime_error("control write");}fs::rename(tmp,path);}
inline void mark(const fs::path& dir,const std::string& run,const std::string& client,const std::string& phase){atomic(dir/(client+"."+phase),run);}
struct Window{int64_t t0,t1;};
inline Window wait(const fs::path& dir,const std::string& run,const std::string& phase){
 auto limit=now()+300LL*1000000000;
 while(now()<limit){if(fs::exists(dir/"STOP"))throw std::runtime_error("coordinator stop");std::ifstream f(dir/phase);std::string id;Window w{};if(f>>id>>w.t0>>w.t1){if(id!=run||w.t1<=w.t0)throw std::runtime_error("stale/invalid window");if(now()>w.t0)throw std::runtime_error("missed start signal");return w;}std::this_thread::sleep_for(std::chrono::milliseconds(20));}
 throw std::runtime_error("control deadline");
}
inline void until(int64_t ns){while(now()<ns)std::this_thread::sleep_for(std::chrono::nanoseconds(std::min<int64_t>(ns-now(),10000000)));}
inline std::string histogram(const std::vector<uint64_t>& h){std::ostringstream o;o<<'[';bool first=true;for(size_t i=0;i<h.size();++i)if(h[i]){if(!first)o<<',';first=false;o<<'['<<i<<','<<h[i]<<']';}o<<']';return o.str();}
}
