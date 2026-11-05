#pragma once
#include <string>

// 读取常规文件并校验大小，返回小写 SHA-256 十六进制字符串。
bool file_sha256(char const* path, long long expected_size,
    std::string& digest);
