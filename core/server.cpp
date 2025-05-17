#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <assert.h>
#include <math.h>

#include <time.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/ip.h>
#include <fcntl.h>
#include <poll.h>

#include <string>
#include <vector>
#include "common.h"
#include "zset/zset.h"
#include "hashtable/hashtable.h"
#include "timer/list.h"

static void msg(const char *msg)
{
    fprintf(stderr, "%s\n", msg);
}

static void msg_errno(const char *msg)
{
    fprintf(stderr, "[errno:%d] %s\n", errno, msg);
}

static void die(const char *msg)
{
    fprintf(stderr, "[%d] %s\n", errno, msg);
    abort();
}

static uint64_t get_monotonic_msec()
{
    struct timespec tv = {0, 0};
    clock_gettime(CLOCK_MONOTONIC, &tv);
    return uint64_t(tv.tv_sec) * 1000 + tv.tv_nsec / 1000 / 1000;
}

static void fd_set_nb(int fd)
{
    errno = 0;
    int flags = fcntl(fd, F_GETFL, 0);
    if (errno)
    {
        die("fcntl error");
        return;
    }
    flags |= O_NONBLOCK;

    errno = 0;
    (void)fcntl(fd, F_SETFL, flags);
    if (errno)
    {
        die("fcntl error");
    }
}

const size_t k_max_msg = 32 << 20;

typedef std::vector<uint8_t> Buffer;
// append to the back
static void
buf_append(Buffer &buf, const uint8_t *data, size_t len)
{
    buf.insert(buf.end(), data, data + len);
}

// remove from the front
static void buf_consume(Buffer &buf, size_t n)
{
    buf.erase(buf.begin(), buf.begin() + n);
}

struct Conn
{
    int fd = -1;
    // application's intention, for the event loop
    bool want_read = false;
    bool want_write = false;
    bool want_close = false;
    // buffered input and output
    Buffer incoming;
    Buffer outgoing;
    // timer
    uint64_t last_active_ms = 0;
    DList idle_node;
};

static struct
{
    HMap db;                     // map of all client connections
    std::vector<Conn *> fd2conn; // fd-conn mapping
    DList idle_list;             // timers of idle connections
} g_data;

// application callback when listening socket is ready
static int32_t *handle_accept(int fd)
{
    // accept
    struct sockaddr_in client_addr = {};
    socklen_t addrlen = sizeof(client_addr);
    int connfd = accept(fd, (struct sockaddr *)&client_addr, &addrlen);
    if (connfd < 0)
    {
        msg_errno("accept() error");
        return NULL;
    }
    uint32_t ip = client_addr.sin_addr.s_addr;
    fprintf(stderr, "new client from %u.%u.%u.%u:%u\n",
            ip & 255, (ip >> 8) & 255, (ip >> 16) & 255, ip >> 24,
            ntohs(client_addr.sin_port));
    fd_set_nb(connfd); // set new connection to nonblocking

    Conn *conn = new Conn();
    conn->fd = connfd;
    conn->want_read = true;
    conn->last_active_ms = get_monotonic_msec();
    dlist_insert_before(&g_data.idle_list, &conn->idle_node);

    if (g_data.fd2conn.size() <= (size_t)connfd)
    {
        g_data.fd2conn.resize(connfd + 1);
    }
    assert(!g_data.fd2conn[connfd]);
    g_data.fd2conn[connfd] = conn;

    return 0;
}

static void conn_destroy(Conn *conn)
{
    (void)close(conn->fd);
    g_data.fd2conn[conn->fd] = NULL;
    dlist_detach(&conn->idle_node);
    delete conn;
}

const size_t k_max_args = 200 * 1000;

static bool read_u32(const uint8_t *&cur, const uint8_t *end, uint32_t &out)
{
    if (cur + 4 > end)
    {
        return false;
    }
    memcpy(&out, cur, 4);
    out = le32toh(out);
    cur += 4;
    return true;
}

static bool
read_str(const uint8_t *&cur, const uint8_t *end, size_t n, std::string &out)
{
    if (cur + n > end)
    {
        return false;
    }
    out.assign(cur, cur + n);
    cur += n;
    return true;
}

// +------+-----+------+-----+------+-----+-----+------+
// | nstr | len | str1 | len | str2 | ... | len | strn |
// +------+-----+------+-----+------+-----+-----+------+

static int32_t
parse_req(const uint8_t *data, size_t size, std::vector<std::string> &out)
{
    // fprintf(stderr, "Raw request data (%zu bytes): ", size);
    // for (size_t i = 0; i < size; i++)
    // {
    //     fprintf(stderr, "%02x ", data[i]);
    // }
    // fprintf(stderr, "\n");
    const uint8_t *end = data + size;
    uint32_t nstr = 0;
    if (!read_u32(data, end, nstr))
    {
        return -1;
    }
    if (nstr > k_max_args)
    {
        return -1;
    }

    while (out.size() < nstr)
    {
        uint32_t len = 0;
        if (!read_u32(data, end, len))
        {
            return -1;
        }

        out.push_back(std::string());
        if (!read_str(data, end, len, out.back()))
        {
            return -1;
        }
    }

    if (data != end)
    {
        fprintf(stderr, "Extra data after parsing command\n");
        return -1;
    }
    return 0;
}

// Response : status
enum
{
    ERR_UNKNOWN = 1, // unknown command
    ERR_TOO_BIG = 2, // response too big
    ERR_BAD_TYP = 3, // bad type
    ERR_BAD_ARG = 4, // bad args
};

// datatypes of serialized data
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

// functions for serialization
static void buf_append_u8(Buffer &buf, uint8_t data)
{
    buf.push_back(data);
}
static void buf_append_u32(Buffer &buf, uint32_t data)
{
    buf_append(buf, (const uint8_t *)&data, 4);
}
static void buf_append_i64(Buffer &buf, int64_t data)
{
    buf_append(buf, (const uint8_t *)&data, 8);
}
static void buf_append_dbl(Buffer &buf, double data)
{
    buf_append(buf, (const uint8_t *)&data, 8);
}

// apend serialized datatypes to the back
static void out_nil(Buffer &out)
{
    buf_append_u8(out, TAG_NIL);
}
static void out_ok(Buffer &out)
{
    buf_append_u8(out, TAG_OK);
}
static void out_str(Buffer &out, const char *s, size_t size)
{
    buf_append_u8(out, TAG_STR);
    buf_append_u32(out, (uint32_t)size);
    buf_append(out, (const uint8_t *)s, size);
}
static void out_int(Buffer &out, int64_t val)
{
    buf_append_u8(out, TAG_INT);
    buf_append_i64(out, val);
}
static void out_dbl(Buffer &out, double val)
{
    buf_append_u8(out, TAG_DBL);
    buf_append_dbl(out, val);
}
static void out_err(Buffer &out, uint32_t code, const std::string &msg)
{
    buf_append_u8(out, TAG_ERR);
    buf_append_u32(out, code);
    buf_append_u32(out, (uint32_t)msg.size());
    buf_append(out, (const uint8_t *)msg.data(), msg.size());
}
static void out_arr(Buffer &out, uint32_t n)
{
    buf_append_u8(out, TAG_ARR);
    buf_append_u32(out, n);
}

static size_t out_begin_arr(Buffer &out)
{
    out.push_back(TAG_ARR);
    buf_append_u32(out, 0);
    return out.size() - 4;
}
static void out_end_arr(Buffer &out, size_t ctx, uint32_t n)
{
    assert(out[ctx - 1] == TAG_ARR);
    memcpy(&out[ctx], &n, 4);
}
// value type
enum
{
    T_INIT = 0,
    T_STR = 1,  // string
    T_ZSET = 2, // sorted set
};

// KV pair for db
struct Entry
{
    struct HNode node;
    std::string key;
    // value
    uint32_t type = 0;
    std::string str;
    ZSet zset;
};

static Entry *entry_new(uint32_t type)
{
    Entry *ent = new Entry();
    ent->type = type;
    return ent;
}

static void entry_del(Entry *ent)
{
    if (ent->type == T_ZSET)
    {
        zset_clear(&ent->zset);
    }
    delete ent;
}

struct LookupKey
{
    struct HNode node;
    std::string key;
};

// eq comparison for struct: Entry
static bool entry_eq(HNode *lhs, HNode *key)
{
    struct Entry *ent = container_of(lhs, struct Entry, node);
    struct LookupKey *hkey = container_of(key, struct LookupKey, node);
    return ent->key == hkey->key;
}

static void do_get(std::vector<std::string> &cmd, Buffer &out)
{
    // dummy struct for lookup
    LookupKey key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());
    // hashtable lookup
    HNode *node = hm_lookup(&g_data.db, &key.node, &entry_eq);
    if (!node)
        return out_nil(out);

    Entry *ent = container_of(node, Entry, node);
    if (ent->type != T_STR)
    {
        return out_err(out, ERR_BAD_TYP, "not a string");
    }
    return out_str(out, ent->str.data(), ent->str.size());
}

static void do_set(std::vector<std::string> &cmd, Buffer &out)
{
    // dummy struct for lookup
    LookupKey key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());
    // hashtable lookup
    HNode *node = hm_lookup(&g_data.db, &key.node, &entry_eq);
    if (node)
    {
        // found, update entry
        Entry *ent = container_of(node, Entry, node);
        if (ent->type != T_STR)
        {
            return out_err(out, ERR_BAD_TYP, "a non string value exists");
        }
        ent->str.swap(cmd[2]);
    }
    else
    {
        // not found, create new entry
        Entry *ent = entry_new(T_STR);
        ent->key.swap(key.key);
        ent->node.hcode = key.node.hcode;
        ent->str.swap(cmd[2]);
        hm_insert(&g_data.db, &ent->node);
    }
    return out_ok(out);
}

static void do_del(std::vector<std::string> &cmd, Buffer &out)
{
    // dummy struct for lookup
    LookupKey key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());
    // hashtable delete
    HNode *node = hm_delete(&g_data.db, &key.node, &entry_eq);
    if (node)
    {
        entry_del(container_of(node, Entry, node));
    }
    return out_int(out, node ? 1 : 0);
}

static bool cb_keys(HNode *node, void *arg)
{
    Buffer &out = *(Buffer *)arg;
    const std::string &key = container_of(node, Entry, node)->key;
    out_str(out, key.data(), key.size());
    return true;
}

static void do_keys(std::vector<std::string> &cmd, Buffer &out)
{
    if (cmd.size() != 1)
    {
        return out_err(out, ERR_UNKNOWN, "KEYS command requires no arguments");
    }
    out_arr(out, (uint32_t)hm_size(&g_data.db));
    hm_foreach(&g_data.db, &cb_keys, (void *)&out);
}

static bool str2dbl(const std::string &s, double &out)
{
    char *endp = NULL;
    out = strtod(s.c_str(), &endp);
    return endp == s.c_str() + s.size() && !isnan(out);
}

static bool str2int(const std::string &s, int64_t &out)
{
    char *endp = NULL;
    out = strtoll(s.c_str(), &endp, 10);
    return endp == s.c_str() + s.size();
}

// zadd zset score name
static void do_zadd(std::vector<std::string> &cmd, Buffer &out)
{
    double score = 0;
    if (!str2dbl(cmd[2], score))
    {
        return out_err(out, ERR_BAD_ARG, "expected float");
    }
    // lookup or create zset
    LookupKey key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());
    HNode *hnode = hm_lookup(&g_data.db, &key.node, &entry_eq);

    Entry *ent = NULL;
    if (!hnode)
    { // insert new key
        ent = entry_new(T_ZSET);
        ent->key.swap(key.key);
        ent->node.hcode = key.node.hcode;
        hm_insert(&g_data.db, &ent->node);
    }
    else
    { // check existing key
        ent = container_of(hnode, Entry, node);
        if (ent->type != T_ZSET)
        {
            return out_err(out, ERR_BAD_TYP, "expect zset");
        }
    }

    // add or update tuple
    const std::string &name = cmd[3];
    bool added = zset_insert(&ent->zset, name.data(), name.size(), score);
    return out_int(out, (int16_t)added);
}

static const ZSet k_empty_zset;

static ZSet *expect_zset(std::string &s)
{
    LookupKey key;
    key.key.swap(s);
    key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());
    HNode *hnode = hm_lookup(&g_data.db, &key.node, &entry_eq);
    if (!hnode)
    { // a non-existent key is treated as an empty zset
        return (ZSet *)&k_empty_zset;
    }
    Entry *ent = container_of(hnode, Entry, node);
    return ent->type == T_ZSET ? &ent->zset : NULL;
}

// zrem zset name
static void do_zrem(std::vector<std::string> &cmd, Buffer &out)
{
    ZSet *zset = expect_zset(cmd[1]);
    if (!zset)
    {
        return out_err(out, ERR_BAD_TYP, "expect zset");
    }
    const std::string &name = cmd[2];
    ZNode *node = zset_lookup(zset, name.data(), name.size());
    if (node)
    {
        zset_delete(zset, node);
    }
    return out_int(out, node ? 1 : 0);
}

// zscore zset name
static void do_zscore(std::vector<std::string> &cmd, Buffer &out)
{
    ZSet *zset = expect_zset(cmd[1]);
    if (!zset)
    {
        return out_err(out, ERR_BAD_TYP, "expect zset");
    }

    const std::string &name = cmd[2];
    ZNode *znode = zset_lookup(zset, name.data(), name.size());
    return znode ? out_dbl(out, znode->score) : out_nil(out);
}

// zquery zset score name offset limit
static void do_zquery(std::vector<std::string> &cmd, Buffer &out)
{
    // parse args
    double score = 0;
    if (!str2dbl(cmd[2], score))
    {
        return out_err(out, ERR_BAD_ARG, "expected float");
    }
    const std::string &name = cmd[3];
    int64_t offset = 0, limit = 0;
    if (!str2int(cmd[4], offset) || !str2int(cmd[5], limit))
    {
        return out_err(out, ERR_BAD_ARG, "expected int");
    }

    // get zset
    ZSet *zset = expect_zset(cmd[1]);
    if (!zset)
    {
        return out_err(out, ERR_BAD_TYP, "expect zset");
    }
    // find znode
    if (limit <= 0)
    {
        return out_arr(out, 0);
    }
    ZNode *znode = zset_seekge(zset, score, name.data(), name.size());
    znode = znode_offset(znode, offset);

    // output
    size_t ctx = out_begin_arr(out);
    int64_t n = 0;
    while (znode && n < limit)
    {
        out_str(out, znode->name, znode->len);
        out_dbl(out, znode->score);
        znode = znode_offset(znode, 1);
        n += 2;
    }
    out_end_arr(out, ctx, (uint32_t)n);
}

static void do_request(std::vector<std::string> &cmd, Buffer &out)
{
    if (cmd.size() == 1 && cmd[0] == "ZAP")
    {
        return out_str(out, "ZING", 4);
    }
    else if (cmd.size() == 2 && cmd[0] == "GET")
    {
        return do_get(cmd, out);
    }
    else if (cmd.size() == 3 && cmd[0] == "SET")
    {
        return do_set(cmd, out);
    }
    else if (cmd.size() == 2 && cmd[0] == "DEL")
    {
        return do_del(cmd, out);
    }
    else if (cmd.size() == 1 && cmd[0] == "KEYS")
    {
        return do_keys(cmd, out);
    }
    else if (cmd.size() == 4 && cmd[0] == "ZADD")
    {
        return do_zadd(cmd, out);
    }
    else if (cmd.size() == 3 && cmd[0] == "ZREM")
    {
        return do_zrem(cmd, out);
    }
    else if (cmd.size() == 3 && cmd[0] == "ZSCORE")
    {
        return do_zscore(cmd, out);
    }
    else if (cmd.size() == 6 && cmd[0] == "ZQUERY")
    {
        return do_zquery(cmd, out);
    }
    else
    {
        return out_err(out, ERR_UNKNOWN, "unknown command");
    }
}

static void response_begin(Buffer &out, size_t *header)
{
    *header = out.size();   // msg header position
    buf_append_u32(out, 0); // reserve space for header
}

static size_t response_size(Buffer &out, size_t header)
{
    return out.size() - header - 4;
}

static void response_end(Buffer &out, size_t header)
{
    size_t msg_size = response_size(out, header);
    if (msg_size > k_max_msg)
    {
        out.resize(header + 4);
        out_err(out, ERR_TOO_BIG, "response is too big, ");
        msg_size = response_size(out, header);
    }
    // copy header
    uint32_t len = (uint32_t)msg_size;
    memcpy(&out[header], &len, 4);
}

// process 1 req if enough data
static bool try_one_request(Conn *conn)
{
    // fprintf(stderr, "try_one_request: Incoming buffer size: %zu\n", conn->incoming.size());
    // for (size_t i = 0; i < conn->incoming.size(); i++)
    // {
    //     fprintf(stderr, "%02x ", conn->incoming[i]);
    // }
    // try to parse header
    if (conn->incoming.size() < 4)
    {
        return false; // not enough data
    }

    uint32_t len = 0;
    memcpy(&len, conn->incoming.data(), 4);

    if (len > k_max_msg)
    {
        msg("too long");
        conn->want_close = true;
        return false;
    }
    size_t expected_size = 4;
    const uint8_t *cur = &conn->incoming[4];
    const uint8_t *end = &conn->incoming[conn->incoming.size()];

    for (uint32_t i = 0; i < len; i++)
    {
        uint32_t str_len = 0;
        if (cur + 4 > end)
        {
            return false;
        }
        memcpy(&str_len, cur, 4);
        str_len = le32toh(str_len);
        cur += 4;

        if (cur + str_len > end)
        {
            return false;
        }
        expected_size += 4 + str_len;
        // fprintf(stderr, "String %u length: %u, Updated expected size: %zu\n", i + 1, str_len, expected_size); // Add 4 bytes for the length and `len` bytes for the string
        cur += str_len;
    }

    if (expected_size > conn->incoming.size())
    {
        return false;
    }

    // body
    if (4 + len > conn->incoming.size())
    {

        return false;
    }
    const uint8_t *request = &conn->incoming[0];

    // got one request, do application logic
    std::vector<std::string> cmd;

    if (parse_req(request, expected_size, cmd) < 0)
    {
        msg("bad request");
        conn->want_close = true;
        return false;
    }
    // fprintf(stderr, "try_one_request: Parsed command:\n");
    // for (size_t i = 0; i < cmd.size(); i++)
    // {
    //     fprintf(stderr, "  Command[%zu]: %s\n", i, cmd[i].c_str());
    // }
    size_t header_pos = 0;
    response_begin(conn->outgoing, &header_pos);

    do_request(cmd, conn->outgoing);

    response_end(conn->outgoing, header_pos);

    buf_consume(conn->incoming, expected_size);
    conn->want_write = true;
    return true;
}

static void handle_write(Conn *conn)
{
    assert(conn->outgoing.size() > 0);
    ssize_t rv = write(conn->fd, &conn->outgoing[0], conn->outgoing.size());
    if (rv < 0 && errno == EAGAIN)
        return; // not ready

    if (rv < 0)
    {
        msg_errno("write() error");
        conn->want_close = true;
        return;
    }
    // remove written data from out buf
    buf_consume(conn->outgoing, (size_t)rv);

    // update readiness if all data written
    if (conn->outgoing.size() == 0)
    {
        conn->want_read = true;
        conn->want_write = false;
    }
}

static void handle_read(Conn *conn)
{
    uint8_t buf[64 * 1024];
    ssize_t rv = read(conn->fd, buf, sizeof(buf));

    if (rv < 0 && errno == EAGAIN)
    {
        return; // not ready
    }
    // handle IO error
    if (rv < 0)
    {
        msg_errno("read() error");
        conn->want_close = true;
        return; // want close
    }

    // handle EOF
    if (rv == 0)
    {
        if (conn->incoming.size() == 0)
        {
            msg("client closed");
        }
        else
        {
            msg("unexpected EOF");
        }
        conn->want_close = true;
        return;
    }
    // got some data
    buf_append(conn->incoming, buf, (size_t)rv);

    // parse requests and generate responses
    while (try_one_request(conn))
    {
    }

    if (conn->outgoing.size() > 0)
    {
        conn->want_read = false;
        conn->want_write = true;
        // The socket is likely ready to write in a request-response protocol,
        // try to write it without waiting for the next iteration.
        return handle_write(conn);
    }
}

const uint64_t k_idle_timeout_ms = 5 * 1000; // 5 seconds

static int32_t next_timer_ms()
{
    if (dlist_empty(&g_data.idle_list))
    {
        return -1;
    }
    uint64_t now_ms = get_monotonic_msec();
    Conn *conn = container_of(g_data.idle_list.next, Conn, idle_node);
    uint64_t next_ms = conn->last_active_ms + k_idle_timeout_ms;
    if (next_ms <= now_ms) // missed timeout
        return 0;
    return (int32_t)next_ms - now_ms;
}

static void process_timers()
{
    uint64_t now_ms = get_monotonic_msec();
    while (!dlist_empty(&g_data.idle_list))
    {
        Conn *conn = container_of(g_data.idle_list.next, Conn, idle_node);
        uint64_t next_ms = conn->last_active_ms + k_idle_timeout_ms;
        if (next_ms >= now_ms)
        {
            break; // not expired
        }
        fprintf(stderr, "iremoving idle connection: %d\n", conn->fd);
        conn_destroy(conn);
    }
}

int main()
{

    // init
    dlist_init(&g_data.idle_list);

    // server listening socket
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
    {
        die("socket()");
    }

    int val = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));

    // bind
    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(1234);
    addr.sin_addr.s_addr = ntohl(INADDR_LOOPBACK);
    int rv = bind(fd, (const struct sockaddr *)&addr, sizeof(addr));
    if (rv)
    {
        die("bind()");
    }

    fd_set_nb(fd); // set to nonblocking

    // listen
    rv = listen(fd, SOMAXCONN);
    if (rv)
    {
        die("listen()");
    }

    // event loop
    std::vector<struct pollfd> poll_args;

    while (true)
    {
        poll_args.clear();
        // put listening socket in 1st position
        struct pollfd pdf = {fd, POLLIN, 0};
        poll_args.push_back(pdf);

        // the rest are connection sockets
        for (Conn *conn : g_data.fd2conn)
        {
            if (!conn)
                continue;

            // always poll() for error
            struct pollfd pfd = {conn->fd, POLLERR, 0};
            // poll() flags from the application's intent
            if (conn->want_read)
            {
                pfd.events |= POLLIN;
            }
            if (conn->want_write)
            {
                pfd.events |= POLLOUT;
            }
            poll_args.push_back(pfd);
        }

        // wait for readiness
        int32_t timeout_ms = next_timer_ms();
        int rv = poll(poll_args.data(), (nfds_t)poll_args.size(), timeout_ms);
        if (rv < 0 && errno == EINTR)
        {
            continue; // not an error
        }
        if (rv < 0)
        {
            die("poll");
        }

        // handle listening socket
        if (poll_args[0].revents)
        {
            handle_accept(fd);
        }

        // handle connection sockets
        for (size_t i = 1; i < poll_args.size(); i++)
        {
            uint32_t ready = poll_args[i].revents;
            if (ready == 0)
                continue;

            Conn *conn = g_data.fd2conn[poll_args[i].fd];

            // update idle timer by moving conn to end of list
            conn->last_active_ms = get_monotonic_msec();
            dlist_detach(&conn->idle_node);
            dlist_insert_before(&g_data.idle_list, &conn->idle_node);

            if (ready & POLLIN)
            {
                assert(conn->want_read);
                handle_read(conn);
            }
            if (ready & POLLOUT)
            {
                assert(conn->want_write);
                handle_write(conn);
            }

            // close socket from err or logic
            if ((ready & POLLERR) || conn->want_close)
            {
                conn_destroy(conn);
            }
        } // for each conn sockets
        // process idle timers
        process_timers();
    }
    return 0;
}
