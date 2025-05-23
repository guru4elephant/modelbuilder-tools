#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Job submitter for batch inference service
"""

import os
import sys
import json
import time
import uuid
from typing import Dict, Any, List, Optional, Tuple

# Try to import qianfan
try:
    import qianfan
    from qianfan.resources.console.data import Data
    HAS_QIANFAN = True
except ImportError:
    HAS_QIANFAN = False

from batch_job_submitter.config import Config
from batch_job_submitter.bos_uploader import BosUploader


class JobSubmitter:
    """Job submitter for batch inference service"""
    
    def __init__(self, config: Config):
        """
        Initialize job submitter
        
        Args:
            config: Configuration
        """
        self.config = config
        
        # Export environment variables
        self.config.export_env_variables()
        
        # Create BOS uploader
        self.uploader = BosUploader(
            access_key_id=self.config.qianfan_access_key,
            secret_access_key=self.config.qianfan_secret_key,
            endpoint=self.config.get("bos", "endpoint", "bj.bcebos.com"),
            bucket=self.config.get("bos", "bucket", "copilot-engine-batch-infer")
        )
        
        # Check qianfan availability
        if not HAS_QIANFAN:
            print("Warning: qianfan package not found. Job submission will fail.", file=sys.stderr)
            
    def upload_file(self, local_file: str) -> Tuple[bool, str]:
        """
        Upload file to BOS
        
        Args:
            local_file: Local file path
            
        Returns:
            Tuple of (success, remote_url)
        """
        return self.uploader.upload(local_file)
        
    def submit_job(
        self, 
        bos_uri: str, 
        job_name: Optional[str] = None,
        description: Optional[str] = None
    ) -> Tuple[bool, str]:
        """
        Submit job to batch inference service
        
        Args:
            bos_uri: BOS URI for input file
            job_name: Job name
            description: Job description
            
        Returns:
            Tuple of (success, task_id)
        """
        if not HAS_QIANFAN:
            raise ImportError("qianfan package not found. Please install it with 'pip install qianfan'")
            
        # Get BOS URI parts
        parts = bos_uri.replace("bos://", "").split("/")
        bucket = parts[0]
        key = "/".join(parts[1:])
        
        # Generate job name if not provided
        if not job_name:
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            unique_id = uuid.uuid4().hex[:8]
            job_name = f"batch_job_{timestamp}_{unique_id}"
            
        # Generate description if not provided
        if not description:
            description = f"Batch job submitted by CLI at {time.strftime('%Y-%m-%d %H:%M:%S')}"
            
        # Get model parameters
        model_id = self.config.get("job", "model_id", "amv-xys3cq1udmud")
        temperature = float(self.config.get("job", "temperature", "0.6"))
        top_p = float(self.config.get("job", "top_p", "0.01"))
        max_output_tokens = int(self.config.get("job", "max_output_tokens", "4096"))
        
        # Extract output URI (same as input but with different folder)
        output_uri_parts = bos_uri.split("/")
        if len(output_uri_parts) >= 4:  # has bucket and at least one folder
            output_uri = "/".join(output_uri_parts[:-1]) + "/output"
        else:
            output_uri = bos_uri + "/output"
            
        try:
            # Submit task
            task = Data.create_offline_batch_inference_task(
                name=job_name,
                descrption=description,
                model_id=model_id,
                inference_params={
                    "temperature": temperature,
                    "top_p": top_p,
                    "max_output_tokens": max_output_tokens
                },
                input_bos_uri=bos_uri,
                output_bos_uri=output_uri
            )
            
            # Extract task ID
            task_id = task['result']['taskId']
            
            return True, task_id
        except Exception as e:
            print(f"Job submission failed: {str(e)}", file=sys.stderr)
            return False, ""
            
    def check_task_status(self, task_id: str) -> Dict[str, Any]:
        """
        Check task status
        
        Args:
            task_id: Task ID
            
        Returns:
            Task status information
        """
        if not HAS_QIANFAN:
            raise ImportError("qianfan package not found. Please install it with 'pip install qianfan'")
            
        try:
            task = Data.describe_batch_inference_task(task_id=task_id)
            return task.get('result', {})
        except Exception as e:
            print(f"Failed to check task status: {str(e)}", file=sys.stderr)
            return {} 