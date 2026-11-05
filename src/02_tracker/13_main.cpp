#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <unistd.h>
#include "01_globals.h"
#include "11_server.h"
int main(int argc, char** argv) {
    const char* config = "../etc/tracker.cfg";
    std::string listen;
    for (int i = 1; i < argc; ++i) {
        if (!strcmp(argv[i], "--config") && i + 1 < argc) config = argv[++i];
        else if (!strcmp(argv[i], "--listen") && i + 1 < argc) listen = argv[++i];
        else if (argv[i][0] != '-' && listen.empty()) listen = argv[i];
        else { fprintf(stderr, "Usage: %s [--config PATH] [--listen IP:PORT]\n", argv[0]); return 1; }
    }
    if (access(config, R_OK)) { perror(config); return 1; }
    acl::acl_cpp_init();
    acl::log::stdout_open(true);
    char* bind_ip = nullptr;
    int bind_port = 21000;
    acl::master_str_tbl strings[] = {{"tnv_listen_ip", "127.0.0.1", &bind_ip}, {0,0,0}};
    acl::master_int_tbl integers[] = {{"tnv_listen_port", 21000, &bind_port, 1, 65535}, {0,0,0,0,0}};
    acl::master_conf pre;
    pre.set_cfg_str(strings); pre.set_cfg_int(integers); pre.load(config);
    if (listen.empty()) listen = std::string(bind_ip) + ":" + std::to_string(bind_port);
    auto colon = listen.rfind(':');
    char* end = nullptr;
    long port = colon == std::string::npos ? 0 : strtol(listen.c_str()+colon+1, &end, 10);
    if (colon == std::string::npos || colon == 0 || !end || *end || port < 1 || port > 65535) {
        fprintf(stderr, "Invalid listening address: %s\n", listen.c_str()); return 1;
    }
    server_c& server = acl::singleton2<server_c>::get_instance();
    server.set_cfg_str(cfg_str); server.set_cfg_int(cfg_int);
    server.run_alone(listen.c_str(), config);
    return 0;
}
