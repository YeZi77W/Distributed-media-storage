#include "13_tracker.h"
#include "01_globals.h"
tracker_c::tracker_c(const char* address):m_stop(false),m_taddr(address){}
void tracker_c::stop(){m_stop=true;m_cv.notify_all();}
void* tracker_c::run(){
    extern char *cfg_node_id,*cfg_data_ip;
    while(!m_stop){
        try{
            m_client=std::make_unique<tnv::RpcControlClient>(m_taddr.c_str());
            bool registered=false;
            while(!m_stop){
                tnv::Reply reply;
                if(!registered){
                    tnv::control::RegisterRequest q;
                    q.set_node_id(cfg_node_id&&*cfg_node_id?cfg_node_id:std::string(cfg_data_ip)+":"+std::to_string(cfg_bindport));
                    q.set_group_name(cfg_gpname);q.set_data_ip(cfg_data_ip);q.set_data_port(cfg_bindport);q.set_version(g_version);q.set_startup_unix_seconds(g_stime);
                    reply=m_client->call("RegisterStorage",q);
                }else{tnv::control::HeartbeatRequest q;q.set_node_id(cfg_node_id&&*cfg_node_id?cfg_node_id:std::string(cfg_data_ip)+":"+std::to_string(cfg_bindport));q.set_group_name(cfg_gpname);reply=m_client->call("Heartbeat",q);}
                if(reply.error!=tnv::rpc::RpcErrorCode::none)break;
                if(reply.status.code()==tnv::control::NOT_FOUND){registered=false;continue;}
                if(reply.status.code()!=tnv::control::OK)break;
                registered=true;
                std::unique_lock<std::mutex> lock(m_mutex);m_cv.wait_for(lock,std::chrono::seconds(cfg_interval),[&]{return m_stop.load();});
            }
        }catch(const std::exception& e){logger_error("Storage RPC: %s",e.what());}
        m_client.reset();
        std::unique_lock<std::mutex> lock(m_mutex);m_cv.wait_for(lock,std::chrono::seconds(2),[&]{return m_stop.load();});
    }
    return nullptr;
}
