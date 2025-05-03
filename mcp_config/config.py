import os
import json
from typing import Dict, Any, Optional

class Config:
    """Configuration management for Photon MCP bridge"""

    _instance = None
    _config = {
        'photon-host': '127.0.0.1',
        'photon-port': 1234,
        'mcp-host': '0.0.0.0',
        'mcp-port': 8000,
        'debug': False
    }

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
            cls._instance.load_config()
        return cls._instance
    
    def load_config(self):
        """Load configuration from file or env"""
        config_path = os.environ.get('PHOTON_MCP_CONFIG', '/config.json')
        try:
            if(os.path.exists(config_path)):
                with open(config_path,'r') as f:
                    self._config.update(json.load(f))
        except Exception as e:
            print(f"Warning : Failed to load config file: {e}")
        
        #Override with environment variables
        for key in self._config.keys():
            env_key = f"PHOTON_MCP_{key.upper()}"
            if env_key in os.environ:
                if isinstance(self._config[key], int):
                    try :
                        self._config[key] = int(os.environ[env_key])
                    except ValueError:
                        self._config[key] = os.environ[env_key]
                else:
                    self._config[key] = os.environ[env_key]

    def get(self, key: str, default: Any = None) -> Any:
        """Get a configuration value"""
        return self._config.get(key, default)
    
    def set(self, key: str, value: Any) -> None:
        """Set a configuration value at runtime"""
        self._config[key] = value