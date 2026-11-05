// 存储服务器
// 实现文件操作类
//
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <lib_acl.hpp>
#include "01_types.h"
#include "07_file.h"

// 构造函数
file_c::file_c(void): m_fd(-1) {
}

// 析构函数
file_c::~file_c(void) {
    close();
}

// 打开文件
int file_c::open(char const* path, char flag) {
    // 检查路径
    if (!path || !*path) {
        logger_error("path is null");
        return ERROR;
    }

    // 打开文件
    if (flag == O_READ)
        m_fd = ::open(path, O_RDONLY | O_NOFOLLOW);
    else if (flag == O_WRITE)
        m_fd = ::open(path, O_WRONLY | O_CREAT | O_EXCL, 0644);
    else {
        logger_error("unknown open flag: %c", flag);
        return ERROR;
    }

    // 打开失败
    if (m_fd == -1) {
        logger_error("call open fail: %s, path: %s, flag: %c",
            strerror(errno), path, flag);
        return ERROR;
    }

    return OK;
}

// 确认文件数据写入共享文件系统
int file_c::sync(void) const {
    if (m_fd == -1) {
        logger_error("sync file fail: invalid file handle");
        return ERROR;
    }
    if (fsync(m_fd) == -1) {
        logger_error("sync file fail: %s", strerror(errno));
        return ERROR;
    }
    return OK;
}

// 关闭文件
int file_c::close(void) {
    if (m_fd != -1) {
        int fd = m_fd;
        m_fd = -1;
        // 关闭文件
        if (::close(fd) == -1) {
            logger_error("call close fail: %s", strerror(errno));
            return ERROR;
        }
    }

    return OK;
}

// 读取文件
int file_c::read(void* buf, size_t count) const {
    // 检查文件描述符
    if (m_fd == -1) {
        logger_error("invalid file handle");
        return ERROR;
    }

    // 检查文件缓冲区
    if (!buf) {
        logger_error("invalid file buffer");
        return ERROR;
    }

    // 常规文件读写也可能短读；读取足量或报告错误。
    size_t done = 0;
    while (done < count) {
        ssize_t bytes = ::read(m_fd, (char*)buf + done, count - done);
        if (bytes < 0 && errno == EINTR)
            continue;
        if (bytes <= 0) {
            if (bytes == 0)
                logger_error("file ended before requested byte count");
            else
                logger_error("read file fail: %s", strerror(errno));
            return ERROR;
        }
        done += bytes;
    }

    return OK;
}

// 写入文件
int file_c::write(void const* buf, size_t count) const {
    // 检查文件描述符
    if (m_fd == -1) {
        logger_error("invalid file handle");
        return ERROR;
    }

    // 检查文件缓冲区
    if (!buf) {
        logger_error("invalid file buffer");
        return ERROR;
    }

    // 共享文件系统也可能发生短写，必须写完本次数据块。
    size_t done = 0;
    while (done < count) {
        ssize_t bytes = ::write(m_fd, (char const*)buf + done, count - done);
        if (bytes < 0 && errno == EINTR)
            continue;
        if (bytes <= 0) {
            if (bytes == 0)
                logger_error("write file made no progress");
            else
                logger_error("write file fail: %s", strerror(errno));
            return ERROR;
        }
        done += bytes;
    }

    return OK;
}

// 设置偏移
int file_c::seek(off_t offset) const {
    // 检查文件描述符
    if (m_fd == -1) {
        logger_error("invalid file handle");
        return ERROR;
    }

    // 检查文件偏移量
    if (offset < 0) {
        logger_error("invalid file offset");
        return ERROR;
    }

    // 设置文件偏移量
    if (lseek(m_fd, offset, SEEK_SET) == -1) {
        logger_error("call lseek fail: %s, offset: %ld",
            strerror(errno), offset);
        return ERROR;
    }

    return OK;
}

// 删除文件
int file_c::del(char const* path) {
    // 检查路径
    if (!path || !*path) {
        logger_error("path is null");
        return ERROR;
    }

    // 删除文件
    if (unlink(path) == -1) {
        logger_error("call unlink fail: %s, path: %s",
            strerror(errno), path);
        return ERROR;
    }

    return OK;
}
