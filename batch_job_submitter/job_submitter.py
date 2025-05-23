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
            bos_uri: BOS URI for input file (should be in format bos:/bucket/key)
            job_name: Job name
            description: Job description
            
        Returns:
            Tuple of (success, task_id)
        """
        if not HAS_QIANFAN:
            raise ImportError("qianfan package not found. Please install it with 'pip install qianfan'")
            
        # Convert bos:// format to bos:/ format if needed
        if bos_uri.startswith("bos://"):
            bos_uri = bos_uri.replace("bos://", "bos:/")
            print(f"Debug: Converted bos:// to bos:/ format: {bos_uri}")
        
        # Ensure BOS URI is in correct format (single slash)
        if not bos_uri.startswith("bos:/"):
            raise ValueError(f"BOS URI must start with 'bos:/', got: {bos_uri}")
            
        # Get BOS URI parts
        parts = bos_uri.replace("bos:/", "").split("/")
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
        
        # Create output URI - use the same path as input for output
        # Following the pattern from the user's example
        output_uri = bos_uri  # Use same path for output
        
        # If the bos_uri is a directory path, ensure it's properly formatted for input/output
        # The API expects the same directory for both input and output
        if bos_uri.endswith('/'):
            # Remove trailing slash if present
            bos_uri = bos_uri.rstrip('/')
            output_uri = bos_uri
            
        # Debug output
        print(f"Debug: Input BOS URI: {bos_uri}")
        print(f"Debug: Output BOS URI: {output_uri}")
        print(f"Debug: Model ID: {model_id}")
        print(f"Debug: Job Name: {job_name}")
        print(f"Debug: Description: {description}")
            
        try:
            # Submit task using the exact parameter names and format from user's example
            inference_params = {
                "temperature": temperature,
                "top_p": top_p,
                "max_output_tokens": max_output_tokens
            }
            
            # Add 'n' parameter if it's a beam search model (optional)
            # This follows the user's example pattern
            if model_id in ["amv-6cg81awp4wu3"]:  # Add beam search model IDs here
                inference_params["n"] = 10
            
            task = Data.create_offline_batch_inference_task(
                name=job_name,
                description=description,  # Fixed spelling
                model_id=model_id,
                inference_params=inference_params,
                input_bos_uri=bos_uri,
                output_bos_uri=output_uri
            )
            
            # Extract task ID
            task_id = task['result']['taskId']
            
            return True, task_id
        except Exception as e:
            print(f"Job submission failed: {str(e)}", file=sys.stderr)
            
            # If it's an InvalidBosUri error, try alternative formats
            if "InvalidBosUri" in str(e):
                print("Debug: Trying alternative BOS URI formats...", file=sys.stderr)
                
                # Try different formats based on user's working example
                alternative_formats = [
                    f"bos:/{bucket}/{key}",  # Single slash format (correct)
                    f"bos://{bucket}/{key}",  # Double slash format
                    f"{bucket}/{key}",  # Without bos prefix
                ]
                
                for alt_input in alternative_formats:
                    alt_output = alt_input  # Use same path for output
                    print(f"Debug: Trying input URI: {alt_input}, output URI: {alt_output}", file=sys.stderr)
                    
                    try:
                        task = Data.create_offline_batch_inference_task(
                            name=job_name,
                            description=description,  # Fixed spelling
                            model_id=model_id,
                            inference_params=inference_params,
                            input_bos_uri=alt_input,
                            output_bos_uri=alt_output
                        )
                        
                        task_id = task['result']['taskId']
                        print(f"Success with alternative format: {alt_input}", file=sys.stderr)
                        return True, task_id
                    except Exception as alt_e:
                        print(f"Alternative format failed: {str(alt_e)}", file=sys.stderr)
                        continue
            
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