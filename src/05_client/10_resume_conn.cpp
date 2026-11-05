// 断点续传协议客户端。
#include <stdlib.h>
#include <string.h>
#include <vector>
#include "01_types.h"
#include "02_proto.h"
#include "03_util.h"
#include "01_conn.h"

int conn_c::upload_request(char command, char const* appid,
    char const* userid, char const* fileid, char const* extra,
    long long extra_length, long long* offset, char* status) {
    if (!appid || !userid || !fileid || !offset || !status ||
        extra_length < 0 || (extra_length && !extra) ||
        extra_length > UPLOAD_CHUNK_MAX + BODYLEN_SIZE * 2 +
            UPLOAD_DIGEST_SIZE)
        return ERROR;
    const size_t identity = APPID_SIZE + USERID_SIZE + FILEID_SIZE;
    std::vector<char> request(HEADLEN + identity + extra_length, 0);
    if (makerequ(command, appid, userid, fileid, &request[0]) != OK)
        return ERROR;
    llton(identity + extra_length, &request[0]);
    if (extra_length)
        memcpy(&request[HEADLEN + identity], extra, extra_length);
    if (!open()) return SOCKET_ERROR;
    size_t sent = 0;
    while (sent < request.size()) {
        long long count = m_conn->write(&request[sent], request.size() - sent);
        if (count <= 0) break;
        sent += count;
    }
    if (sent != request.size()) {
        m_errdesc = "send upload request fail";
        close();
        return SOCKET_ERROR;
    }
    char* body = NULL;
    long long bodylen = 0;
    int result = recvbody(&body, &bodylen);
    if (result == OK) {
        if (bodylen != BODYLEN_SIZE + 1) {
            m_errdesc = "invalid upload progress reply";
            result = ERROR;
            close();
        } else {
            *offset = ntoll(body);
            *status = body[BODYLEN_SIZE];
        }
    } else if (result == STATUS_ERROR) {
        m_errnumb = body && bodylen >= ERROR_NUMB_SIZE ? ntos(body) : -1;
        m_errdesc = body && bodylen > ERROR_NUMB_SIZE ?
            body + ERROR_NUMB_SIZE : "upload request rejected";
    }
    free(body);
    return result;
}

int conn_c::upload_init(char const* appid, char const* userid,
    char const* fileid, long long size, unsigned char const* digest,
    long long* offset, char* status) {
    if (size < 0 || !digest) return ERROR;
    char extra[BODYLEN_SIZE + UPLOAD_DIGEST_SIZE];
    llton(size, extra);
    memcpy(extra + BODYLEN_SIZE, digest, UPLOAD_DIGEST_SIZE);
    return upload_request(CMD_STORAGE_UPLOAD_INIT, appid, userid, fileid,
        extra, sizeof(extra), offset, status);
}

int conn_c::upload_status(char const* appid, char const* userid,
    char const* fileid, long long* offset, char* status) {
    return upload_request(CMD_STORAGE_UPLOAD_STATUS, appid, userid, fileid,
        NULL, 0, offset, status);
}

int conn_c::upload_chunk(char const* appid, char const* userid,
    char const* fileid, long long offset, char const* data,
    long long length, long long* confirmed) {
    if (offset < 0 || length <= 0 || length > UPLOAD_CHUNK_MAX || !data)
        return ERROR;
    std::vector<char> extra(BODYLEN_SIZE * 2 + length);
    llton(offset, &extra[0]);
    llton(length, &extra[BODYLEN_SIZE]);
    memcpy(&extra[BODYLEN_SIZE * 2], data, length);
    char status = 0;
    return upload_request(CMD_STORAGE_UPLOAD_CHUNK, appid, userid, fileid,
        &extra[0], extra.size(), confirmed, &status);
}

int conn_c::upload_commit(char const* appid, char const* userid,
    char const* fileid, long long* confirmed, char* status) {
    return upload_request(CMD_STORAGE_UPLOAD_COMMIT, appid, userid, fileid,
        NULL, 0, confirmed, status);
}
