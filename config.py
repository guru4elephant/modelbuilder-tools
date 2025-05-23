#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Configuration handler for batch job submitter
"""

import os
import json
import configparser
from pathlib import Path
from typing import Dict, Any, Optional

DEFAULT_CONFIG_FILE = "~/.batch_job_submitter.ini"
DEFAULT_CONFIG = {
    "qianfan": {
        "access_key": "",
        "secret_key": "",
        "host": "qianfan.baidubce.com"
    },
    "bos": {
        "endpoint": "bj.bcebos.com",
        "bucket": "copilot-engine-batch-infer"
    },
    "job": {
        "model_id": "amv-xys3cq1udmud",
        "temperature": 0.6,
        "top_p": 0.01,
        "max_output_tokens": 4096
    }
}


class Config:
    """Configuration manager for batch job submitter"""

    def __init__(self, config_file: Optional[str] = None):
        """
        Initialize configuration
        
        Args:
            config_file: Path to configuration file (optional)
        """
        self.config_file = config_file or os.path.expanduser(DEFAULT_CONFIG_FILE)
        self.config = self._load_config()

    def _load_config(self) -> Dict[str, Dict[str, Any]]:
        """Load configuration from file or use defaults"""
        config_path = Path(self.config_file)
        
        if not config_path.exists():
            return DEFAULT_CONFIG.copy()
            
        config = configparser.ConfigParser()
        config.read(config_path)
        
        result = DEFAULT_CONFIG.copy()
        
        # Update with values from config file
        for section in config.sections():
            if section in result:
                for key, value in config[section].items():
                    result[section][key] = value
                    
        return result

    def save_config(self) -> None:
        """Save current configuration to file"""
        config_path = Path(self.config_file)
        
        # Create directory if it doesn't exist
        config_path.parent.mkdir(parents=True, exist_ok=True)
        
        config = configparser.ConfigParser()
        
        for section, values in self.config.items():
            config[section] = values
            
        with open(config_path, 'w') as f:
            config.write(f)
            
    def get(self, section: str, key: str, default: Any = None) -> Any:
        """Get configuration value"""
        try:
            return self.config[section][key]
        except (KeyError, TypeError):
            return default
            
    def set(self, section: str, key: str, value: Any) -> None:
        """Set configuration value"""
        if section not in self.config:
            self.config[section] = {}
            
        self.config[section][key] = value
        
    def export_env_variables(self) -> None:
        """Export configuration as environment variables"""
        os.environ["QIANFAN_ACCESS_KEY"] = self.get("qianfan", "access_key", "")
        os.environ["QIANFAN_SECRET_KEY"] = self.get("qianfan", "secret_key", "")
        
    @property
    def qianfan_access_key(self) -> str:
        """Get Qianfan access key"""
        return self.get("qianfan", "access_key", "")
        
    @property
    def qianfan_secret_key(self) -> str:
        """Get Qianfan secret key"""
        return self.get("qianfan", "secret_key", "") 