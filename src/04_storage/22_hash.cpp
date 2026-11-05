#include <errno.h>
#include <fcntl.h>
#include <openssl/sha.h>
#include <sys/stat.h>
#include <unistd.h>
#include "22_hash.h"

bool file_sha256(char const* path, long long expected_size,
    std::string& digest) {
    if (!path || expected_size < 0) return false;
    int fd = open(path, O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
    if (fd == -1) return false;
    struct stat info;
    bool good = !fstat(fd, &info) && S_ISREG(info.st_mode) &&
        info.st_size == expected_size;
    SHA256_CTX context;
    if (good) good = SHA256_Init(&context) == 1;
    char buffer[65536];
    long long total = 0;
    while (good && total < expected_size) {
        size_t wanted = expected_size - total < (long long)sizeof(buffer) ?
            expected_size - total : sizeof(buffer);
        ssize_t count = read(fd, buffer, wanted);
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) { good = false; break; }
        good = SHA256_Update(&context, buffer, count) == 1;
        total += count;
    }
    unsigned char result[SHA256_DIGEST_LENGTH];
    if (good) good = SHA256_Final(result, &context) == 1;
    if (close(fd)) good = false;
    if (!good) return false;
    static char const hex[] = "0123456789abcdef";
    digest.resize(SHA256_DIGEST_LENGTH * 2);
    for (size_t i = 0; i < SHA256_DIGEST_LENGTH; ++i) {
        digest[i * 2] = hex[result[i] >> 4];
        digest[i * 2 + 1] = hex[result[i] & 15];
    }
    return true;
}
