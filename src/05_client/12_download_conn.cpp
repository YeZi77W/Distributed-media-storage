#include <stdlib.h>
#include <string.h>
#include "01_types.h"
#include "02_proto.h"
#include "03_util.h"
#include "01_conn.h"

int conn_c::filemeta(char const* appid, char const* userid,
    char const* fileid, long long* filesize, std::string& digest) {
    if (!appid || !userid || !fileid || !filesize) return ERROR;
    const size_t identity = APPID_SIZE + USERID_SIZE + FILEID_SIZE;
    char request[HEADLEN + identity] = {};
    if (makerequ(CMD_STORAGE_FILEMETA, appid, userid, fileid,
        request) != OK) return ERROR;
    llton(identity, request);
    if (!open()) return SOCKET_ERROR;
    size_t sent = 0;
    while (sent < sizeof(request)) {
        long long count = m_conn->write(request + sent, sizeof(request) - sent);
        if (count <= 0) { close(); return SOCKET_ERROR; }
        sent += count;
    }
    char* body = NULL;
    long long bodylen = 0;
    int result = recvbody(&body, &bodylen);
    if (result == OK) {
        if (bodylen != BODYLEN_SIZE + FILE_DIGEST_HEX_SIZE) {
            m_errdesc = "invalid file metadata length";
            result = ERROR;
            close();
        } else {
            *filesize = ntoll(body);
            digest.assign(body + BODYLEN_SIZE, FILE_DIGEST_HEX_SIZE);
            if (*filesize < 0) result = ERROR;
            for (size_t i = 0; i < digest.size(); ++i)
                if (!((digest[i] >= '0' && digest[i] <= '9') ||
                      (digest[i] >= 'a' && digest[i] <= 'f')))
                    result = ERROR;
        }
    } else if (result == STATUS_ERROR) {
        m_errnumb = body && bodylen >= ERROR_NUMB_SIZE ? ntos(body) : -1;
        m_errdesc = body && bodylen > ERROR_NUMB_SIZE ?
            body + ERROR_NUMB_SIZE : "file metadata rejected";
    }
    free(body);
    return result;
}
