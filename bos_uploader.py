#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
BOS (Baidu Object Storage) file uploader
"""

import os
import sys
import uuid
from pathlib import Path
from typing import Tuple, List, Dict, Optional

# Try to import the BOS helper
try:
    from jsonflow.utils.bos import BosHelper, upload_file
    HAS_JSONFLOW = True
except ImportError:
    HAS_JSONFLOW = False
    
# Fallback implementation for BosHelper and upload_file if jsonflow not available
if not HAS_JSONFLOW:
    import boto3
    from botocore.exceptions import ClientError
    
    class BosHelper:
        def __init__(self, access_key_id: str, secret_access_key: str, endpoint: str, bucket: str):
            self.access_key_id = access_key_id
            self.secret_access_key = secret_access_key
            self.endpoint = endpoint
            self.bucket = bucket
            
            # Create BOS client
            self.client = boto3.client(
                's3',
                aws_access_key_id=access_key_id,
                aws_secret_access_key=secret_access_key,
                endpoint_url=f'https://{endpoint}'
            )
    
    def upload_file(
        local_file: str,
        remote_key: str,
        bucket: str,
        access_key_id: Optional[str] = None,
        secret_access_key: Optional[str] = None
    ) -> Tuple[bool, str]:
        """Upload file to BOS"""
        try:
            # Get credentials from environment if not provided
            ak = access_key_id or os.environ.get("QIANFAN_ACCESS_KEY")
            sk = secret_access_key or os.environ.get("QIANFAN_SECRET_KEY")
            
            if not ak or not sk:
                raise ValueError("Access key and secret key must be provided")
                
            endpoint = os.environ.get("BOS_ENDPOINT", "bj.bcebos.com")
            
            # Create BOS client
            client = boto3.client(
                's3',
                aws_access_key_id=ak,
                aws_secret_access_key=sk,
                endpoint_url=f'https://{endpoint}'
            )
            
            # Upload file
            client.upload_file(local_file, bucket, remote_key)
            
            # Construct URL
            remote_url = f"bos://{bucket}/{remote_key}"
            
            return True, remote_url
        except Exception as e:
            print(f"Upload failed: {str(e)}", file=sys.stderr)
            return False, ""


class BosUploader:
    """BOS file uploader"""
    
    def __init__(
        self,
        access_key_id: str,
        secret_access_key: str,
        endpoint: str = "bj.bcebos.com",
        bucket: str = "copilot-engine-batch-infer"
    ):
        """
        Initialize BOS uploader
        
        Args:
            access_key_id: BOS access key ID
            secret_access_key: BOS secret access key
            endpoint: BOS endpoint
            bucket: BOS bucket name
        """
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.endpoint = endpoint
        self.bucket = bucket
        
        if HAS_JSONFLOW:
            self.helper = BosHelper(
                access_key_id=access_key_id,
                secret_access_key=secret_access_key,
                endpoint=endpoint,
                bucket=bucket
            )
        
    def upload(self, local_file: str, remote_dir: str = "llm-algo") -> Tuple[bool, str]:
        """
        Upload file to BOS
        
        Args:
            local_file: Local file path
            remote_dir: Remote directory
            
        Returns:
            Tuple of (success, remote_url)
        """
        # Get file name
        file_name = Path(local_file).name
        
        # Generate a unique key for the file
        unique_id = uuid.uuid4().hex[:8]
        remote_key = f"{remote_dir}/{unique_id}/{file_name}"
        
        success, remote_url = upload_file(
            local_file=local_file,
            remote_key=remote_key,
            bucket=self.bucket,
            access_key_id=self.access_key_id,
            secret_access_key=self.secret_access_key
        )
        
        return success, remote_url 