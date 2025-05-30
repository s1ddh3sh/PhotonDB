# ⚡PhotonDB

**PhotonDB** is a high-performance in-memory key-value database server written in C++17. It provides access to mutable data structures via a simple TCP-based protocol using a server-client model.

PhotonDB also integrates with [MCP](https://modelcontextprotocol.io/) — the Model Context Protocol — enabling natural-language interactions with the data, making it more than just a database (actively under development).

<details open>
<summary><h2>Using PhotonDB with photon-cli</h2> </summary>

`photon-cli` is PhotonDB's command line interface

You can start a photon server instance `./src/server`
and then, in another terminal try the following:

```sh
% cd src/
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

<details open>
<summary><h2>Building PhotonDB</h2> </summary>

PhotonDB uses [CMake](https://cmake.org/) for building. Make sure you have CMake (version 3.10 or newer) and a C++17 compiler installed.

```sh
#Clone the repository and enter the directory
git clone https://github.com/s1ddh3sh/PhotonDB.git
cd PhotonDB

#Create a build directory and run CMake
mkdir build
cd build
cmake ..

#Build the server and CLI
make
```

Make sure to run the `make` command in the build dir to compile the changes and build the server/client

</details>

#### API Reference

Please refer the [`API.md`](https://github.com/s1ddh3sh/PhotonDB/blob/main/API.md) for full list of supported commands and usage examples
