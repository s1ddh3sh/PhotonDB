# ⚡PhotonDB

**PhotonDB** is a high-performance in-memory key-value database server written in C++17. It provides access to mutable data structures via a simple TCP-based protocol using a server-client model.

PhotonDB also integrates with [MCP](https://modelcontextprotocol.io/) — the Model Context Protocol — enabling natural-language interactions with the data, making it more than just a database (actively under development).

<details open>
<summary><h2>Using PhotonDB with photon-cli</h2> </summary>

`photon-cli` is PhotonDB's command line interface

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

</details>

<details open>
<summary><h2>Using PhotonDB with MCP client</h2></summary>

PhotonDB also supports the Model Context Protocol (MCP) for natural-language interactions. The MCP server is implemented in `server.py`

### Adding MCP to your project

We recommend using [uv](https://docs.astral.sh/uv/) as a python package manager

```
uv add "mcp[cli]"
```

To run the mcp command with uv :

```
uv run mcp
```

You can install this `server.py` on [Claude Desktop](https://claude.ai/download) and interact with it right away by running :

```
uv run mcp install server.py
```

</details>

#### API Reference

Please refer the `API.md` for full list of supported commands and usage examples
