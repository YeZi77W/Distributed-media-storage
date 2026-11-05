// 存储服务器共享卷检查
#include <limits.h>
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include "01_types.h"
#include "01_globals.h"
#include "18_volume.h"

namespace {

struct volume_t {
    std::string path;
    dev_t device;
    ino_t inode;
    int fd; // 保持挂载卷引用，避免常规卸载后误写入本地目录。
};

std::vector<volume_t> volumes;

bool safe_root_char(char ch) {
    return (ch >= 'a' && ch <= 'z') ||
        (ch >= 'A' && ch <= 'Z') ||
        (ch >= '0' && ch <= '9') ||
        ch == '/' || ch == '_' || ch == '-' || ch == '.';
}

// /proc/self/mountinfo 对空格、反斜杠等字符使用八进制转义。
std::string unescape_mount_path(std::string const& encoded) {
    std::string decoded;
    for (size_t i = 0; i < encoded.size(); ++i) {
        if (encoded[i] == '\\' && i + 3 < encoded.size() &&
            encoded[i + 1] >= '0' && encoded[i + 1] <= '7' &&
            encoded[i + 2] >= '0' && encoded[i + 2] <= '7' &&
            encoded[i + 3] >= '0' && encoded[i + 3] <= '7') {
            int value = (encoded[i + 1] - '0') * 64 +
                (encoded[i + 2] - '0') * 8 + encoded[i + 3] - '0';
            decoded += static_cast<char>(value);
            i += 3;
        }
        else
            decoded += encoded[i];
    }
    return decoded;
}

bool is_mount_point(std::string const& path) {
    std::ifstream mounts("/proc/self/mountinfo");
    if (!mounts)
        return false;

    std::string line;
    while (std::getline(mounts, line)) {
        std::istringstream fields(line);
        std::string id, parent, device, root, mount;
        if (fields >> id >> parent >> device >> root >> mount)
            if (unescape_mount_path(mount) == path)
                return true;
    }
    return false;
}

bool volume_ready(volume_t const& volume) {
    struct stat info;
    if (!is_mount_point(volume.path) ||
        stat(volume.path.c_str(), &info) || !S_ISDIR(info.st_mode) ||
        info.st_dev != volume.device || info.st_ino != volume.inode ||
        access(volume.path.c_str(), R_OK | W_OK | X_OK)) {
        logger_error("shared volume unavailable or changed: %s",
            volume.path.c_str());
        return false;
    }
    return true;
}

bool has_dot_component(std::string const& path) {
    size_t begin = 0;
    while (begin < path.size()) {
        size_t end = path.find('/', begin);
        std::string component = path.substr(begin, end - begin);
        if ((component.empty() && begin != 0) ||
            component == "." || component == "..")
            return true;
        if (end == std::string::npos)
            break;
        begin = end + 1;
    }
    return false;
}

} // namespace

int prepare_shared_storage(void) {
    for (std::vector<volume_t>::const_iterator volume = volumes.begin();
        volume != volumes.end(); ++volume)
        close(volume->fd);
    volumes.clear();
    for (std::vector<std::string>::const_iterator path = g_spaths.begin();
        path != g_spaths.end(); ++path) {
        if (path->empty() || (*path)[0] != '/' || *path == "/" ||
            path->size() > ID_KEY_MAX) {
            logger_error("storage root must be an absolute mount point of at "
                "most %d bytes: %s", ID_KEY_MAX, path->c_str());
            return ERROR;
        }
        for (size_t i = 0; i < path->size(); ++i)
            if (!safe_root_char((*path)[i])) {
                logger_error("storage root has unsupported characters: %s",
                    path->c_str());
                return ERROR;
            }

        char canonical[PATH_MAX];
        if (!realpath(path->c_str(), canonical) || *path != canonical) {
            logger_error("storage root must be canonical: %s", path->c_str());
            return ERROR;
        }

        if (!is_mount_point(*path)) {
            logger_error("storage root is not mounted: %s", path->c_str());
            return ERROR;
        }

        struct stat info;
        if (stat(path->c_str(), &info) || !S_ISDIR(info.st_mode) ||
            access(path->c_str(), R_OK | W_OK | X_OK)) {
            logger_error("storage root is not a readable writable directory: %s",
                path->c_str());
            return ERROR;
        }

        for (std::vector<volume_t>::const_iterator prior = volumes.begin();
            prior != volumes.end(); ++prior)
            if (prior->path == *path) {
                logger_error("duplicate storage root: %s", path->c_str());
                return ERROR;
            }

        int fd = open(path->c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (fd == -1) {
            logger_error("open shared storage root fail: %s", path->c_str());
            return ERROR;
        }
        struct stat pinned;
        if (fstat(fd, &pinned) || pinned.st_dev != info.st_dev ||
            pinned.st_ino != info.st_ino || !is_mount_point(*path)) {
            close(fd);
            logger_error("shared storage root changed while starting: %s",
                path->c_str());
            return ERROR;
        }
        volume_t volume = {*path, info.st_dev, info.st_ino, fd};
        std::string staging = *path + "/.staging";
        int created = mkdir(staging.c_str(), 0755) == 0;
        if (!created && errno != EEXIST) {
            close(fd);
            logger_error("create shared staging directory fail: %s",
                staging.c_str());
            return ERROR;
        }
        struct stat staged;
        if (lstat(staging.c_str(), &staged) || !S_ISDIR(staged.st_mode) ||
            access(staging.c_str(), R_OK | W_OK | X_OK)) {
            close(fd);
            logger_error("shared staging directory is unusable: %s",
                staging.c_str());
            return ERROR;
        }
        if (created && fsync(fd)) {
            close(fd);
            logger_error("sync shared staging parent fail: %s",
                path->c_str());
            return ERROR;
        }
        volumes.push_back(volume);
        logger("shared storage root ready: %s", path->c_str());
    }
    return volumes.empty() ? ERROR : OK;
}

bool shared_root_ready(char const* root) {
    if (!root)
        return false;
    for (std::vector<volume_t>::const_iterator volume = volumes.begin();
        volume != volumes.end(); ++volume)
        if (volume->path == root)
            return volume_ready(*volume);
    logger_error("unknown shared storage root: %s", root);
    return false;
}

bool shared_path_ready(char const* filepath) {
    if (!filepath)
        return false;
    std::string path(filepath);
    if (path.empty() || path[path.size() - 1] == '/' ||
        has_dot_component(path)) {
        logger_error("invalid shared storage path: %s", filepath);
        return false;
    }
    std::string parent = path.substr(0, path.find_last_of('/'));
    char canonical[PATH_MAX];
    if (!realpath(parent.c_str(), canonical) || parent != canonical) {
        logger_error("file parent is not canonical: %s", filepath);
        return false;
    }
    for (std::vector<volume_t>::const_iterator volume = volumes.begin();
        volume != volumes.end(); ++volume)
        if (path.size() > volume->path.size() &&
            path.compare(0, volume->path.size(), volume->path) == 0 &&
            path[volume->path.size()] == '/')
            return volume_ready(*volume);
    logger_error("file path is outside shared storage roots: %s", filepath);
    return false;
}
