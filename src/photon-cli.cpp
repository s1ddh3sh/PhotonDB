#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/ip.h>
#include <assert.h>
#include <vector>
#include <string>
#include <iostream>
#include <algorithm>
#define PROMPT "\033[93m⚡photon>\033[0m "

static void msg(const char *msg)
{
    fprintf(stderr, "%s\n", msg);
}

static void die(const char *msg)
{
    int err = errno;
    fprintf(stderr, "[%d] %s\n", err, msg);
    abort();
}

static int32_t read_full(int fd, char *buf, size_t n)
{
    while (n > 0)
    {
        ssize_t rv = read(fd, buf, n);
        if (rv <= 0)
            return -1; // error or unexpected EOF
        assert((size_t)rv <= n);
        n -= (size_t)rv;
        buf += rv;
    }
    return 0;
}

static int32_t write_all(int fd, char *buf, size_t n)
{
    size_t written = 0;
    while (n > 0)
    {
        ssize_t rv = write(fd, buf, n);
        if (rv <= 0)
            return -1; // error
        assert((size_t)rv <= n);
        n -= (size_t)rv;
        buf += rv;
        written += (size_t)rv;
    }
    return 0;
}

const ssize_t k_max_msg = 4096; // likely bigger than kernel buffer

static int32_t send_req(int fd, const std::vector<std::string> &cmd)
{
    std::vector<std::string> uppercased_cmd = cmd;
    if (!uppercased_cmd.empty())
    {
        std::string &first_cmd = uppercased_cmd[0];
        std::transform(first_cmd.begin(), first_cmd.end(), first_cmd.begin(), ::toupper);
    }
    uint32_t len = 0;
    uint32_t n = uppercased_cmd.size();
    size_t cur = 0;
    for (const std::string &s : uppercased_cmd)
    {
        len += s.size();
    }
    if (len > k_max_msg)
        return -1;

    char wbuf[4 + k_max_msg];
    n = htole32(n);
    memcpy(&wbuf[cur], &n, 4);
    cur += 4;
    for (const std::string &s : uppercased_cmd)
    {
        uint32_t p = htole32((uint32_t)s.size());
        memcpy(&wbuf[cur], &p, 4);
        memcpy(&wbuf[cur + 4], s.data(), s.size());
        cur += 4 + s.size();
    }
    // fprintf(stderr, "Sending request:\n");
    // for (size_t i = 0; i < cur; i++)
    // {
    //     fprintf(stderr, "%02x ", (unsigned char)wbuf[i]);
    // }
    // fprintf(stderr, "\n");
    return write_all(fd, wbuf, cur);
}

enum
{
    TAG_NIL = 0, // nil
    TAG_ERR = 1, // error code + msg
    TAG_STR = 2, // string
    TAG_INT = 3, // int64
    TAG_DBL = 4, // double
    TAG_ARR = 5, // array
    TAG_OK = 6   // success response
};

static int32_t print_response(const uint8_t *data, size_t size)
{
    if (size < 1)
    {
        msg("bad response");
        return -1;
    }
    switch (data[0])
    {
    case TAG_NIL:
        printf("not found\n");
        return 1;
    case TAG_OK:
        printf("OK\n");
        return 1;
    case TAG_ERR:
        if (size < 1 + 8)
        {
            msg("bad_response");
            return -1;
        }
        {
            int32_t code = 0;
            uint32_t len = 0;
            memcpy(&code, &data[1], 4);
            code = le32toh(code);
            memcpy(&len, &data[1 + 4], 4);
            len = le32toh(len);
            if (size < 1 + 8 + len)
            {
                msg("bad_response");
                return -1;
            }
            printf("(err) %d %.*s\n", code, len, &data[1 + 8]);
            return 1 + 8 + len;
        }
    case TAG_STR:
        if (size < 1 + 4)
        {
            msg("bad response");
            return -1;
        }
        {
            uint32_t len = 0;
            memcpy(&len, &data[1], 4);
            len = le32toh(len);
            if (size < 1 + 4 + len)
            {
                msg("bad response");
                return -1;
            }
            printf("(str) %.*s\n", len, &data[1 + 4]);
            return 1 + 4 + len;
        }
    case TAG_INT:
        if (size < 1 + 8)
        {
            msg("bad response");
            return -1;
        }
        {
            int64_t val = 0;
            memcpy(&val, &data[1], 8);
            val = le64toh(val);
            printf("(int) %ld\n", val);
            return 1 + 8;
        }
    case TAG_DBL:
        if (size < 1 + 8)
        {
            msg("bad response");
            return -1;
        }
        {
            double val = 0;
            memcpy(&val, &data[1], 8);
            printf("(dbl) %g\n", val);
            return 1 + 8;
        }

    case TAG_ARR:
        if (size < 1 + 4)
        {
            msg("bad response");
            return -1;
        }
        {
            uint32_t len = 0;
            memcpy(&len, &data[1], 4);
            len = le32toh(len);
            printf("(arr) len=%u\n", len);
            size_t arr_bytes = 1 + 4;
            for (uint32_t i = 0; i < len; i++)
            {
                int32_t rv = print_response(&data[arr_bytes], size - arr_bytes);
                if (rv < 0)
                {
                    return rv;
                }
                arr_bytes += (size_t)rv;
            }
            printf("(arr) end\n");
            return (int32_t)arr_bytes;
        }
    default:
        msg("bad response");
        return -1;
    }
}

static int32_t read_res(int fd)
{
    // 4B header
    char rbuf[4 + k_max_msg + 1];
    errno = 0;
    int32_t err = read_full(fd, rbuf, 4);
    if (err)
    {
        if (errno == 0)
            msg("EOF");
        else
            msg("read() error");
        return err;
    }

    uint32_t len = 0;
    memcpy(&len, rbuf, 4);
    len = le32toh(len);
    if (len > k_max_msg)
    {
        msg("response is too long");
        return -1;
    }

    // reply body
    err = read_full(fd, &rbuf[4], len);
    if (err)
    {
        msg("read() error");
        return err;
    }

    // print server response
    int32_t rv = print_response((uint8_t *)&rbuf[4], len);
    if (rv < 0)
    {
        msg("failed to parse response");
        return rv;
    }
    if ((uint32_t)rv != len)
    {
        fprintf(stderr, "DEBUG: response size mismatch %d != %u\n", rv, len);
    }
    return 0;
}

int main()
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
    {
        die("socket()");
    }

    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = ntohs(1234);
    addr.sin_addr.s_addr = ntohl(INADDR_LOOPBACK);
    int rv = connect(fd, (const struct sockaddr *)&addr, sizeof(addr));

    if (rv)
    {
        die("connect");
    }

    while (true)
    {
        printf(PROMPT);
        fflush(stdout);

        std::string line;
        if (!std::getline(std::cin, line))
        {
            printf("\n");
            break; // EOF
        }
        std::vector<std::string> cmd;

        for (size_t pos = 0; pos < line.size();)
        {
            if (isspace(line[pos]))
            {
                pos++;
                continue;
            }
            size_t start = pos;
            while (pos < line.size() && !isspace(line[pos]))
            {
                pos++;
            }
            cmd.push_back(line.substr(start, pos - start));
        }
        if (cmd.empty())
            continue;
        if (cmd[0] == "exit")
            break;

        int32_t err = send_req(fd, cmd);
        if (err)
        {
            msg("Failed to send request");
            break;
        }
        err = read_res(fd);
        if (err)
        {
            msg("Failed to send request");
            break;
        }
    }
    close(fd);
    return 0;
}