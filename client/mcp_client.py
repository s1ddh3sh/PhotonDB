import socket
import struct
from typing import List, Dict, Any,Union, Optional

from .protocol import (
    TAG_NIL,TAG_STR,TAG_INT,TAG_DBL,TAG_ARR,TAG_ERR,
    format_command,parse_response
)
from .config import Config

class PhotonMCPClient:
    """Client to connect C++ Photon server"""

    def __init__(self,host: Optional[str] = None, port: Optional[int] = None):
        self.config = Config()
        self.host = host or self.config.get('photon-host', '127.0.0.1')
        self.port = port or self.config.get('photon-port',1234)

    def _connect(self) -> socket.socket:
        """Establish connection with Photon server"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.host, self.port))
        return sock
    
    def execute_command(self, *args: str) -> Any:
        """Execute command and return the result"""
        sock = self._connect()
        try:
            msg = format_command(list(args))
            sock.sendall(msg)

            # Read the response
            header = sock.recv(4)
            if len(header) < 4:
                raise ConnectionError("Incomplete header received")
            
            msg_len, = struct.unpack('!I', header)

            #Read the full msg
            data = b''
            remaining = msg_len
            while remaining > 0:
                chunk = sock.recv(min(remaining, 4096))
                if not chunk:
                    raise ConnectionError("Connection closed by server")
                data += chunk
                remaining -= len(chunk)
            return parse_response(data)
        finally:
            sock.close()


    def get(self, key:str) -> Optional[str]:
        return self.execute_command('GET', key)
    
    def set(self, key:str, value:str) -> None:
        self.execute_command('SET', key, value)

    def delete(self, key:str) -> int:
        return self.execute_command('DEL', key)
    
    def keys(self) -> List[str]:
        """GET all keys from db"""
        return self.execute_command('KEYS')