from typing import List, Dict, Any, Optional
from mcp.server.fastmcp import FastMCP

from mcp_client import PhotonMCPClient
from mcp_config.config import Config

config = Config()
app = FastMCP('PhotonDB')
client = PhotonMCPClient()

@app.tool()
def get(key:str) -> Optional[str]:
    return client.get(key)

@app.tool()
def set(key:str, value:str) -> None:
    return client.set(key, value)

@app.tool()
def delete(key:str) -> int:
    return client.delete(key)

@app.tool()
def keys() -> List[str]:
    return client.keys()


#Register default resources
@app.resource("info://status")
def get_status()-> Dict[str,Any]:
    try:
        key_count = len(client.keys())
        return {
            "status": "online",
            "key_count": key_count,
            "photon-host": client.host,
            "photon-port": client.port
        }
    except Exception as e:
        return {
            "status": "offline",
            "error": str(e)
        }
