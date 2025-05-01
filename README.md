# ⚡PhotonDB

**PhotonDB** is a lightweight, high-performance in-memory key-value database server written in vanilla C++17.
It provides access to mutable data structures via a set of commands, which are sent using a server-client model with TCP sockets and a simple protocol.

What sets PhotonDB apart from its conventional CLI to interact with, is its native support for [MCP](https://modelcontextprotocol.io/) — the Model Context Protocol — enabling seamless, natural-language interactions with the database engine. With MCP, PhotonDB becomes more than a data store — it becomes a conversational system that understands high-level user intents in context (actively under development).

## Using Photon with photon-cli

`photon-cli` is Photon's command line interface

You can start a photon server instance `./core/server.cpp`
and then, in another terminal try the following:

```sh
% cd core/
% ./photon-cli
⚡photon> ZAP
 (str) ZING
⚡photon> set foo bar
 OK
⚡photon> get foo
 (str) bar
```
