#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <assert.h>
#include <math.h>
#include <fstream>

#include <time.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/ip.h>
#include <fcntl.h>
#include <poll.h>
#include <mutex>

#include <string>
#include <vector>

#include "common.h"
#include "zset.h"
#include "hashtable.h"
#include "list.h"
#include "heap.h"
#include "thread_pool.h"
#include "commands/commands.h"

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
    std::vector<HeapItem> heap;  // timers for key TTLs
    ThreadPool thread_pool;      // thread pool
} g_data;

std::mutex snap_mutex;

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
void out_err(Buffer &out, uint32_t code, const std::string &msg)
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
    size_t heap_idx = -1; // index of this entry in the heap
};

static Entry *entry_new(uint32_t type)
{
    Entry *ent = new Entry();
    ent->type = type;
    return ent;
}

static void entry_del_sync(Entry *ent)
{
    if (ent->type == T_ZSET)
    {
        zset_clear(&ent->zset);
    }
    delete ent;
}

static void entry_del_func(void *arg)
{
    entry_del_sync((Entry *)arg);
}

static void entry_set_ttl(Entry *ent, int64_t ttl_ms);

static void entry_del(Entry *ent)
{
    // unlink it from any data structure
    entry_set_ttl(ent, -1); // soft delete ent from heap

    // run destructor in threadpool only for larger size zset
    size_t set_size = (ent->type == T_ZSET) ? hm_size(&ent->zset.hmap) : 0;
    const size_t k_large_container_size = 1000;
    if (set_size > k_large_container_size)
    {
        thread_pool_queue(&g_data.thread_pool, &entry_del_func, ent);
    }
    else
    {
        entry_del_sync(ent);
    }
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

void do_get(std::vector<std::string> &cmd, Buffer &out)
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

void do_set(std::vector<std::string> &cmd, Buffer &out)
{
    std::lock_guard<std::mutex> lk(snap_mutex);

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

void do_del(std::vector<std::string> &cmd, Buffer &out)
{
    std::lock_guard<std::mutex> lk(snap_mutex);
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

static void heap_delete(std::vector<HeapItem> &heap, size_t pos)
{
    // swap with last
    heap[pos] = heap.back();
    heap.pop_back();

    if (pos < heap.size())
        heap_update(heap.data(), pos, heap.size());
}

static void heap_upsert(std::vector<HeapItem> &heap, size_t pos, HeapItem t)
{
    if (pos < heap.size())
        heap[pos] = t; // update exising
    else
    {
        pos = heap.size();
        heap.push_back(t); // insert new
    }
    heap_update(heap.data(), pos, heap.size());
}

// set or remove TTL
static void entry_set_ttl(Entry *ent, int64_t ttl_ms)
{
    if (ttl_ms < 0 && ent->heap_idx != (size_t)-1)
    {
        heap_delete(g_data.heap, ent->heap_idx);
        ent->heap_idx = (size_t)-1; // soft delete (-1)
    }
    else if (ttl_ms >= 0)
    {
        // add/update heap
        uint64_t expire_at = get_monotonic_msec() + (uint64_t)ttl_ms;
        HeapItem item = {expire_at, &ent->heap_idx};
        heap_upsert(g_data.heap, ent->heap_idx, item);
    }
}
static bool str2int(const std::string &s, int64_t &out)
{
    char *endp = NULL;
    out = strtoll(s.c_str(), &endp, 10);
    return endp == s.c_str() + s.size();
}

// PEXPIRE key ttl_ms
void do_expire(std::vector<std::string> &cmd, Buffer &out)
{
    int64_t ttl_ms = 0;
    if (!str2int(cmd[2], ttl_ms))
    {
        return out_err(out, ERR_BAD_ARG, "expected int64");
    }
    LookupKey key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());

    HNode *node = hm_lookup(&g_data.db, &key.node, &entry_eq);
    if (node)
    {
        Entry *ent = container_of(node, Entry, node);
        entry_set_ttl(ent, ttl_ms);
    }
    return out_int(out, node ? 1 : 0);
}

// PTTL key
void do_ttl(std::vector<std::string> &cmd, Buffer &out)
{
    LookupKey key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());

    HNode *node = hm_lookup(&g_data.db, &key.node, &entry_eq);
    if (!node)
    {
        return out_int(out, -2); // key not found
    }
    Entry *ent = container_of(node, Entry, node);
    if (ent->heap_idx == (size_t)-1)
    {
        return out_int(out, -1); // no TTL
    }
    uint64_t expire_at = g_data.heap[ent->heap_idx].val;
    uint64_t now_ms = get_monotonic_msec();
    return out_int(out, expire_at > now_ms ? (expire_at - now_ms) : 0);
}

static bool cb_keys(HNode *node, void *arg)
{
    Buffer &out = *(Buffer *)arg;
    const std::string &key = container_of(node, Entry, node)->key;
    out_str(out, key.data(), key.size());
    return true;
}

void do_keys(std::vector<std::string> &cmd, Buffer &out)
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

// zadd zset score name
void do_zadd(std::vector<std::string> &cmd, Buffer &out)
{
    std::lock_guard<std::mutex> lk(snap_mutex);
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
void do_zrem(std::vector<std::string> &cmd, Buffer &out)
{
    std::lock_guard<std::mutex> lk(snap_mutex);
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
void do_zscore(std::vector<std::string> &cmd, Buffer &out)
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
void do_zquery(std::vector<std::string> &cmd, Buffer &out)
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
void save_zset(std::ofstream &out, ZSet *zset)
{
    uint32_t count = (uint32_t)hm_size(&zset->hmap);
    out.write((char *)&count, sizeof(count));
    zset_foreach(zset, [](ZNode *z, void *arg)
                 {
        std::ofstream &out = *(std::ofstream *)arg;
        out.write((char *)&z->score, sizeof(z->score));
        uint32_t len = (uint32_t)z->len;
        out.write((char*)&len, sizeof(len));
        out.write(z->name, len); }, &out);
}
static void load_zset(std::ifstream &in, ZSet *zset)
{
    uint32_t count = 0;
    in.read((char *)&count, sizeof(count));
    for (uint32_t i = 0; i < count; i++)
    {
        double score = 0;
        in.read((char *)&score, sizeof(score));
        uint32_t len = 0;
        in.read((char *)&len, sizeof(len));
        std::string name(len, '\0');
        in.read(&name[0], len);
        zset_insert(zset, name.data(), len, score);
    }
}
static bool save_snapshot(const char *filename)
{
    std::lock_guard<std::mutex> lk(snap_mutex);
    std::ofstream out(filename, std::ios::binary | std::ios::trunc);
    if (!out)
        return false;
    uint32_t n = (uint32_t)hm_size(&g_data.db);
    out.write((char *)&n, sizeof(n));
    hm_foreach(&g_data.db, [](HNode *node, void *arg)
               {
        std::ofstream &out = *(std::ofstream *)arg;
        Entry *ent = container_of(node, Entry, node);

        uint32_t klen = (uint32_t)ent->key.size();
        out.write((char*)&klen, sizeof(klen));
        out.write(ent->key.data(), klen);

        out.write((char*)&ent->type, sizeof(ent->type));
        if(ent->type == T_STR) {
            uint32_t vlen = (uint32_t)ent->str.size();
            out.write((char*)&vlen, sizeof(vlen));
            out.write(ent->str.data(), vlen);
        }
        else if(ent->type == T_ZSET) { 
            save_zset(out, &ent->zset);
        }
        return true; }, &out);
    return true;
}

void do_save(std::vector<std::string> &, Buffer &out)
{
    if (save_snapshot("photon.rdb"))
        out_ok(out);
    else
        out_err(out, ERR_UNKNOWN, "save failed");
}
static bool load_snapshot(const char *filename)
{
    std::lock_guard<std::mutex> lk(snap_mutex);

    std::ifstream in(filename, std::ios::binary);
    if (!in)
        return false;

    // clear current db
    hm_clear(&g_data.db);
    uint32_t n = 0;
    in.read((char *)&n, sizeof(n));
    for (uint32_t i = 0; i < n; i++)
    {
        uint32_t klen = 0;
        in.read((char *)&klen, sizeof(klen));
        std::string key(klen, '\0');
        in.read(&key[0], klen);

        uint32_t type = 0;
        in.read((char *)type, sizeof(type));
        Entry *ent = entry_new(type);
        ent->key = key;
        ent->node.hcode = str_hash((uint8_t *)key.data(), key.size());

        if (type == T_STR)
        {
            uint32_t vlen = 0;
            in.read((char *)vlen, sizeof(vlen));
            ent->str.resize(vlen);
            in.read(&ent->str[0], vlen);
        }
        else if (type == T_ZSET)
        {
            load_zset(in, &ent->zset);
        }
        hm_insert(&g_data.db, &ent->node);
    }
    return true;
}
void do_load(std::vector<std::string> &, Buffer &out)
{
    if (load_snapshot("photon.rdb"))
        out_ok(out);
    else
        out_err(out, ERR_UNKNOWN, "load failed");
}

void do_zap(std::vector<std::string> &, Buffer &out)
{
    out_str(out, "ZING", 4);
}

// void do_request(std::vector<std::string> &cmd, Buffer &out)
// {
//     if (cmd.size() == 1 && cmd[0] == "ZAP")
//     {
//         return out_str(out, "ZING", 4);
//     }
//     else if (cmd.size() == 2 && cmd[0] == "GET")
//     {
//         return do_get(cmd, out);
//     }
//     else if (cmd.size() == 3 && cmd[0] == "SET")
//     {
//         return do_set(cmd, out);
//     }
//     else if (cmd.size() == 2 && cmd[0] == "DEL")
//     {
//         return do_del(cmd, out);
//     }
//     else if (cmd.size() == 1 && cmd[0] == "KEYS")
//     {
//         return do_keys(cmd, out);
//     }
//     else if (cmd.size() == 4 && cmd[0] == "ZADD")
//     {
//         return do_zadd(cmd, out);
//     }
//     else if (cmd.size() == 3 && cmd[0] == "ZREM")
//     {
//         return do_zrem(cmd, out);
//     }
//     else if (cmd.size() == 3 && cmd[0] == "ZSCORE")
//     {
//         return do_zscore(cmd, out);
//     }
//     else if (cmd.size() == 6 && cmd[0] == "ZQUERY")
//     {
//         return do_zquery(cmd, out);
//     }
//     else if (cmd.size() == 3 && cmd[0] == "PEXPIRE")
//     {
//         return do_expire(cmd, out);
//     }
//     else if (cmd.size() == 2 && cmd[0] == "PTTL")
//     {
//         return do_ttl(cmd, out);
//     }
//     else if (cmd.size() == 1 && cmd[0] == "SAVE")
//     {
//         if (save_snapshot("photon.rdb"))
//             return out_ok(out);
//         else
//             return out_err(out, ERR_UNKNOWN, "save failed");
//     }
//     else if (cmd.size() == 1 && cmd[0] == "LOAD")
//     {
//         if (load_snapshot("photon.rdb"))
//             return out_ok(out);
//         else
//             return out_err(out, ERR_UNKNOWN, "load failed");
//     }
//     else
//     {
//         return out_err(out, ERR_UNKNOWN, "unknown command");
//     }
// }

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

    // update idle timer only on actual activity (r/w)
    conn->last_active_ms = get_monotonic_msec();
    dlist_detach(&conn->idle_node);
    dlist_insert_before(&g_data.idle_list, &conn->idle_node);

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

const uint64_t k_idle_timeout_ms = 20 * 1000; // 5 seconds

static uint32_t next_timer_ms()
{
    uint64_t now_ms = get_monotonic_msec();
    uint64_t next_ms = (uint64_t)-1;
    // idle timers using linked list
    if (!dlist_empty(&g_data.idle_list))
    {
        Conn *conn = container_of(g_data.idle_list.next, Conn, idle_node);
        next_ms = conn->last_active_ms + k_idle_timeout_ms;
    }
    // TTL using heap
    if (!g_data.heap.empty() && g_data.heap[0].val < next_ms)
    {
        next_ms = g_data.heap[0].val;
    }
    if (next_ms == (uint64_t)-1)
        return -1;

    if (next_ms <= now_ms) // missed timeout
        return 0;
    return (int32_t)next_ms - now_ms;
}

static bool hnode_same(HNode *node, HNode *key)
{
    return node == key;
}

static void process_timers()
{
    uint64_t now_ms = get_monotonic_msec();
    // idle timers using linked list
    while (!dlist_empty(&g_data.idle_list))
    {
        Conn *conn = container_of(g_data.idle_list.next, Conn, idle_node);
        uint64_t next_ms = conn->last_active_ms + k_idle_timeout_ms;
        if (next_ms >= now_ms)
        {
            break; // not expired
        }
        fprintf(stderr, "removing idle connection: %d\n", conn->fd);
        conn_destroy(conn);
    }
    // TTL using heap
    const size_t k_max_works = 2000; // to limit the number of keys expiring at same time
    size_t nworks = 0;
    const std::vector<HeapItem> &heap = g_data.heap;
    while (!heap.empty() && heap[0].val < now_ms)
    {
        Entry *ent = container_of(heap[0].ref, Entry, heap_idx);
        HNode *node = hm_delete(&g_data.db, &ent->node, &hnode_same);
        assert(node == &ent->node);
        // delete key
        entry_del(ent);
        if (nworks++ > k_max_works)
        {
            break;
        }
    }
}

static void save_snap_task(void *arg)
{
    const char *filename = (const char *)arg;
    save_snapshot(filename);
}

int main()
{
    // init
    dlist_init(&g_data.idle_list);
    thread_pool_init(&g_data.thread_pool, 4);

    load_snapshot("photon.rdb");
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

            // // update idle timer by moving conn to end of list
            // conn->last_active_ms = get_monotonic_msec();
            // dlist_detach(&conn->idle_node);
            // dlist_insert_before(&g_data.idle_list, &conn->idle_node);

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