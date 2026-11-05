// HTTP服务器
// 实现业务服务类
//
#include <algorithm>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include "01_types.h"
#include "02_proto.h"
#include "03_util.h"
#include "07_client.h"
#include "01_status.h"
#include "02_globals.h"
#include "04_service.h"

#define ROUTE_FILES "/files/"
#define APPID       "tnvideo"
#define USERID      "tnv001"
#define FILE_SLICE  (1024*1024LL)

namespace {
bool decimal(char const* begin, char const* end, long long& value) {
    if (begin == end) return false;
    value = 0;
    for (char const* p = begin; p != end; ++p) {
        if (*p < '0' || *p > '9' || value > (LLONG_MAX - (*p - '0')) / 10)
            return false;
        value = value * 10 + (*p - '0');
    }
    return true;
}

// HTTP 字节区间末端为包含关系；仅支持一个区间。
bool parse_range(char const* header, long long total,
    long long& from, long long& to) {
    if (!header || strncmp(header, "bytes=", 6) || total <= 0)
        return false;
    char const* first = header + 6;
    char const* dash = strchr(first, '-');
    if (!dash || strchr(dash + 1, '-') || strchr(first, ',')) return false;
    char const* end = first + strlen(first);
    if (first == dash) {
        long long suffix = 0;
        if (!decimal(dash + 1, end, suffix) || suffix == 0) return false;
        from = suffix >= total ? 0 : total - suffix;
        to = total - 1;
        return true;
    }
    if (!decimal(first, dash, from) || from >= total) return false;
    if (dash + 1 == end) to = total - 1;
    else {
        if (!decimal(dash + 1, end, to) || to < from) return false;
        to = std::min(to, total - 1);
    }
    return true;
}
}

service_c::service_c(acl::socket_stream* conn, acl::session* sess):
    HttpServlet(conn, sess) {
}

bool service_c::doGet(acl::HttpServletRequest& req,
    acl::HttpServletResponse& res) {
    acl::string path = req.getPathInfo();
    if (!path.ncompare(ROUTE_FILES, strlen(ROUTE_FILES)))
        // 处理文件路由
        return files(req, res);
    else {
        logger_error("unknown route, path: %s", path.c_str());
        res.setStatus(STATUS_BAD_REQUEST);
    }

    // 发送响应
    return res.write(NULL, 0);
}

bool service_c::doPost(acl::HttpServletRequest& req,
    acl::HttpServletResponse& res) {
    // 发送响应
    return res.write(NULL, 0);
}

bool service_c::doOptions(acl::HttpServletRequest& req,
    acl::HttpServletResponse& res) {
    res.setStatus(STATUS_OK)
       .setContentType("text/plain;charset=utf-8")
       .setContentLength(0)
       .setKeepAlive(req.isKeepAlive());

    // 发送响应
    return res.write(NULL, 0);
}

bool service_c::doError(acl::HttpServletRequest& req,
    acl::HttpServletResponse& res) {
    acl::string body;
    body.format("<root error='some error happened' />\r\n");
    res.setStatus(STATUS_BAD_REQUEST)
       .setContentType("text/html;charset=utf-8")
       .setContentLength(body.size());
    return res.write(body);
}

bool service_c::doOther(acl::HttpServletRequest& req,
    acl::HttpServletResponse& res, char const* method) {
    acl::string body;
    body.format("<root error='unknown request method %s' />\r\n", method);
    res.setStatus(STATUS_BAD_REQUEST)
       .setContentType("text/html;charset=utf-8")
       .setContentLength(body.size());
    return res.write(body);
}

////////////////////////////////////////////////////////////////////////

// 处理文件路由
bool service_c::files(acl::HttpServletRequest& req,
    acl::HttpServletResponse& res) {
    res.setKeepAlive(req.isKeepAlive());
    acl::string path = req.getPathInfo();
    acl::string fileid = path.right(strlen(ROUTE_FILES));
    if (!fileid.c_str() || !fileid.size() ||
        fileid.size() >= FILEID_SIZE || valid(fileid.c_str()) != OK) {
        res.setStatus(STATUS_BAD_REQUEST).setContentLength(0);
        return res.write(NULL, 0);
    }
    res.setContentType("application/octet-stream");
    acl::string filename;
    filename.format("attachment;filename=%s", fileid.c_str());
    res.setHeader("content-disposition", filename.c_str());
    client_c client;
    long long filesize = 0;
    std::string digest;
    if (client.filemeta(APPID, USERID, fileid.c_str(),
        &filesize, digest) != OK) {
        res.setStatus(STATUS_INTER_SERVER_ERROR).setContentLength(0);
        return res.write(NULL, 0);
    }
    acl::string etag;
    etag.format("\"%s\"", digest.c_str());
    res.setHeader("ETag", etag.c_str());
    res.setHeader("Accept-Ranges", "bytes");
    long long range_from = 0;
    long long range_to = filesize ? filesize - 1 : -1;
    char const* range = req.getHeader("Range");
    char const* if_range = req.getHeader("If-Range");
    bool partial = range && !strncmp(range, "bytes=", 6) &&
        !strchr(range, ',') &&
        (!if_range || !strcmp(if_range, etag.c_str()));
    if (partial && !parse_range(range, filesize, range_from, range_to)) {
        acl::string invalid;
        invalid.format("bytes */%lld", filesize);
        res.setHeader("Content-Range", invalid.c_str());
        res.setStatus(STATUS_RANGE_UNSATISFIABLE).setContentLength(0);
        return res.write(NULL, 0);
    }
    long long remain = partial ? range_to - range_from + 1 : filesize;
    if (partial) {
        acl::string selected;
        selected.format("bytes %lld-%lld/%lld", range_from, range_to,
            filesize);
        res.setStatus(STATUS_PARTIAL_CONTENT);
        res.setHeader("Content-Range", selected.c_str());
    } else res.setStatus(STATUS_OK);
    res.setContentLength(remain);
    long long offset = range_from;
    while (remain) {
        long long size = std::min(remain, FILE_SLICE);
        char* downdata = NULL;
        long long downsize = 0;
        if (client.download(APPID, USERID, fileid.c_str(),
            offset, size, &downdata, &downsize) != OK ||
            downsize != size || !downdata) {
            free(downdata);
            return false;
        }
        bool wrote = res.write(downdata, downsize);
        free(downdata);
        if (!wrote) return false;
        remain -= downsize;
        offset += downsize;
    }
    return res.write(NULL, 0);
}
