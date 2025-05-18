#include "commands.h"
#include "../zset.h"
#include "../hashtable.h"
#include <unordered_map>
#include <algorithm>

using CommandHandler = void (*)(std::vector<std::string> &, Buffer &);

struct CommandEntry
{
    CommandHandler handler;
    size_t min_args;
    size_t max_args;
};

static const std::unordered_map<std::string, CommandEntry> command_table = {
    {"ZAP", {do_zap, 1, 1}},
    {"GET", {do_get, 2, 2}},
    {"SET", {do_set, 3, 3}},
    {"DEL", {do_del, 2, 2}},
    {"KEYS", {do_keys, 1, 1}},
    {"ZADD", {do_zadd, 4, 4}},
    {"ZREM", {do_zrem, 3, 3}},
    {"ZSCORE", {do_zscore, 3, 3}},
    {"ZQUERY", {do_zquery, 6, 6}},
    {"PEXPIRE", {do_expire, 3, 3}},
    {"PTTL", {do_ttl, 2, 2}},
    {"SAVE", {do_save, 1, 1}},
    {"LOAD", {do_load, 1, 1}},
};

void do_request(std::vector<std::string> &cmd, Buffer &out)
{
    if (cmd.empty())
    {
        return out_err(out, ERR_UNKNOWN, "empty command");
    }
    std::string cmd_name = cmd[0];
    std::transform(cmd_name.begin(), cmd_name.end(), cmd_name.begin(), ::toupper);

    auto it = command_table.find(cmd_name);
    if (it == command_table.end())
    {
        return out_err(out, ERR_UNKNOWN, "unknown command");
    }
    const CommandEntry &entry = it->second;
    if (cmd.size() < entry.min_args || cmd.size() > entry.max_args)
    {
        return out_err(out, ERR_BAD_ARG, "wrong number of arguments");
    }
    entry.handler(cmd, out);
}