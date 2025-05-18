#pragma once
#include <vector>
#include <string>
#include "../common.h"

enum
{
    ERR_UNKNOWN = 1, // unknown command
    ERR_TOO_BIG = 2, // response too big
    ERR_BAD_TYP = 3, // bad type
    ERR_BAD_ARG = 4, // bad args
};

typedef std::vector<uint8_t> Buffer;

extern void do_zap(std::vector<std::string> &, Buffer &);
extern void do_get(std::vector<std::string> &, Buffer &);
extern void do_set(std::vector<std::string> &, Buffer &);
extern void do_del(std::vector<std::string> &, Buffer &);
extern void do_keys(std::vector<std::string> &, Buffer &);
extern void do_zadd(std::vector<std::string> &, Buffer &);
extern void do_zrem(std::vector<std::string> &, Buffer &);
extern void do_zscore(std::vector<std::string> &, Buffer &);
extern void do_zquery(std::vector<std::string> &, Buffer &);
extern void do_expire(std::vector<std::string> &, Buffer &);
extern void do_ttl(std::vector<std::string> &, Buffer &);
extern void do_save(std::vector<std::string> &, Buffer &);
extern void do_load(std::vector<std::string> &, Buffer &);

void do_request(std::vector<std::string> &cmd, Buffer &out);
void out_err(Buffer &out, uint32_t code, const std::string &msg);