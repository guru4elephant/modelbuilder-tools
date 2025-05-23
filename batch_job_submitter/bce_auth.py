#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
BCE API signature tool for authentication
"""

import hashlib
import hmac
import urllib.parse
from datetime import datetime, timezone
import requests
from typing import Dict, Any, Optional


class BceApiSignatureTool:
    """BCE API signature tool for generating authentication headers"""
    
    def __init__(self, ak: str, sk: str, host: str = "qianfan.baidubce.com"):
        """
        Initialize BCE API signature tool
        
        Args:
            ak: Access key
            sk: Secret key  
            host: API host
        """
        self.ak = ak
        self.sk = sk
        self.host = host
        
    def _get_canonical_time(self, timestamp: Optional[datetime] = None) -> str:
        """Get canonical time string"""
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)
        return timestamp.strftime('%Y-%m-%dT%H:%M:%SZ')
        
    def _get_auth_string(self, method: str, uri: str, query: str, headers: Dict[str, str], timestamp: str) -> str:
        """Generate BCE auth string"""
        # Canonical request
        canonical_uri = urllib.parse.quote(uri, safe='/')
        canonical_query = query
        
        # Sort headers
        signed_headers = sorted(headers.keys())
        canonical_headers = '\n'.join(f'{k}:{headers[k]}' for k in signed_headers)
        signed_headers_str = ';'.join(signed_headers)
        
        canonical_request = f"{method}\n{canonical_uri}\n{canonical_query}\n{canonical_headers}"
        
        # String to sign
        string_to_sign = f"bce-auth-v1/{self.ak}/{timestamp}/1800/{signed_headers_str}/{hashlib.sha256(canonical_request.encode()).hexdigest()}"
        
        # Calculate signature
        signing_key = hmac.new(self.sk.encode(), timestamp.encode(), hashlib.sha256).digest()
        signature = hmac.new(signing_key, string_to_sign.encode(), hashlib.sha256).hexdigest()
        
        return f"bce-auth-v1/{self.ak}/{timestamp}/1800/{signed_headers_str}/{signature}"
        
    def post(self, uri: str, query: str = "", body: Optional[Dict[str, Any]] = None, timeout: int = 30) -> requests.Response:
        """
        Send POST request with BCE authentication
        
        Args:
            uri: API URI path
            query: Query string
            body: Request body
            timeout: Request timeout
            
        Returns:
            Response object
        """
        url = f"https://{self.host}{uri}"
        if query:
            url += f"?{query}"
            
        timestamp = self._get_canonical_time()
        
        headers = {
            'Content-Type': 'application/json',
            'Host': self.host
        }
        
        # Generate auth string
        auth_string = self._get_auth_string('POST', uri, query, headers, timestamp)
        headers['Authorization'] = auth_string
        
        return requests.post(url, headers=headers, json=body, timeout=timeout) 