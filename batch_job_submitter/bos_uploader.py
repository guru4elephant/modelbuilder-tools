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
            
            # Construct URL using correct single slash format
            remote_url = f"bos:/{bucket}/{remote_key}"
            
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
            Tuple of (success, remote_url) where remote_url is BOS URI format pointing to directory
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
        
        if success:
            # Convert HTTPS URL to BOS URI format if needed
            if remote_url.startswith("https://"):
                # Extract the path part from HTTPS URL
                # Example: https://copilot-engine-batch-infer.bj.bcebos.com/llm-algo/5306ceb2/deepmath-part8-trial1.jsonl
                # Should become: bos:/copilot-engine-batch-infer/llm-algo/5306ceb2/
                parts = remote_url.split('/')
                if len(parts) >= 4:
                    bucket_name = parts[2].split('.')[0]  # Extract bucket from domain
                    path_parts = parts[3:]  # Get path parts after domain
                    # Remove filename to get directory path
                    if len(path_parts) > 0:
                        dir_path = '/'.join(path_parts[:-1])  # Remove last part (filename)
                        remote_url = f"bos:/{bucket_name}/{dir_path}"
            
            # Ensure we return directory path, not file path
            elif remote_url.startswith("bos:/"):
                # If it's already BOS format, ensure it points to directory
                if '/' in remote_url:
                    parts = remote_url.split('/')
                    if len(parts) > 3:  # Has filename at the end
                        # Remove filename to get directory
                        dir_path = '/'.join(parts[:-1])
                        remote_url = dir_path
            
            print(f"Debug: Converted to BOS directory URI: {remote_url}")
        
        return success, remote_url 