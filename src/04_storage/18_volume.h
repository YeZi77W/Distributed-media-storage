// 存储服务器共享卷检查
#pragma once

// 在启动时校验并记录所有共享卷挂载点。
int prepare_shared_storage(void);

// 请求处理时确认挂载点仍是启动时的卷。
bool shared_root_ready(char const* root);
bool shared_path_ready(char const* filepath);
