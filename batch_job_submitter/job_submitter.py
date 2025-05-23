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
import requests
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
from batch_job_submitter.bce_auth import BceApiSignatureTool


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
        
        # Create BCE API tool for direct API calls
        self.api_tool = BceApiSignatureTool(
            ak=self.config.qianfan_access_key,
            sk=self.config.qianfan_secret_key,
            host=self.config.get("qianfan", "host", "qianfan.baidubce.com")
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
            
            # Create task without description parameter - following user's working example
            task = Data.create_offline_batch_inference_task(
                name=job_name,
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
        Check task status using direct BCE API calls
        
        Args:
            task_id: Task ID
            
        Returns:
            Task status information
        """
        try:
            # Use the same pattern as list_tasks but for a single task
            payload = {
                "taskId": task_id
            }
            
            # Make API call using BCE authentication
            response = self.api_tool.post(
                uri="/v2/batchinference",
                query="Action=DescribeBatchInferenceTask",  # Singular, not plural
                body=payload
            )
            
            response.raise_for_status()
            response_data = response.json()
            
            # Extract task result
            result = response_data.get("result", {})
            
            # Return the task information in the same format as list_tasks
            task_info = {
                'taskId': result.get('taskId'),
                'name': result.get('name'),
                'status': result.get('runStatus'),
                'progress': result.get('progress', 0),
                'createTime': result.get('createTime'),
                'startTime': result.get('startTime'),
                'endTime': result.get('endTime'),
                'inputBosUri': result.get('inputBosUri'),
                'outputBosUri': result.get('outputBosUri'),
                'outputDir': result.get('outputDir'),
                'modelId': result.get('modelId'),
                'errorCode': result.get('errorCode'),
                'errorMessage': result.get('errorMessage')
            }
            
            return task_info
            
        except requests.exceptions.RequestException as e:
            print(f"API request failed: {str(e)}", file=sys.stderr)
            return {}
        except json.JSONDecodeError as e:
            print(f"Failed to parse API response: {str(e)}", file=sys.stderr)
            return {}
        except Exception as e:
            print(f"Failed to check task status: {str(e)}", file=sys.stderr)
            return {}
            
    def list_tasks(self, limit: int = 20, offset: int = 0, run_status: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        List batch inference tasks using direct BCE API calls with pagination
        
        Args:
            limit: Maximum number of tasks to return (default: 20)
            offset: Offset for pagination (default: 0)
            run_status: List of task statuses to filter (e.g., ["Done", "Running"])
            
        Returns:
            List of task information
        """
        try:
            all_tasks = []
            marker = ""
            tasks_collected = 0
            
            # Default to all statuses if not specified
            if run_status is None:
                run_status = ["Done", "Running", "Failed", "Cancelled"]
            
            while tasks_collected < limit:
                payload = {
                    "runStatus": run_status,
                    "pageReverse": True  # Get most recent tasks first
                }
                
                # Add marker for pagination (except for first request)
                if marker:
                    payload["marker"] = marker
                
                try:
                    # Make API call using BCE authentication
                    response = self.api_tool.post(
                        uri="/v2/batchinference",
                        query="Action=DescribeBatchInferenceTasks",
                        body=payload
                    )
                    
                    response.raise_for_status()
                    response_data = response.json()
                    
                    result = response_data.get("result", {})
                    task_list = result.get("taskList", [])
                    page_info = result.get("pageInfo", {})
                    is_truncated = page_info.get("isTruncated", False)
                    
                    # Process tasks
                    for task in task_list:
                        if tasks_collected >= limit:
                            break
                            
                        # Extract relevant task information
                        task_info = {
                            'taskId': task.get('taskId'),
                            'name': task.get('name'),
                            'status': task.get('runStatus'),
                            'progress': task.get('progress', 0),
                            'createTime': task.get('createTime'),
                            'startTime': task.get('startTime'),
                            'endTime': task.get('endTime'),
                            'inputBosUri': task.get('inputBosUri'),
                            'outputBosUri': task.get('outputBosUri'),
                            'outputDir': task.get('outputDir'),
                            'modelId': task.get('modelId')
                        }
                        
                        all_tasks.append(task_info)
                        tasks_collected += 1
                        
                        # Update marker for next page
                        marker = task.get('taskId')
                    
                    # Break if no more pages or no tasks returned
                    if not is_truncated or not task_list:
                        break
                        
                    # Small delay between requests
                    time.sleep(0.1)
                    
                except requests.exceptions.RequestException as e:
                    print(f"API request failed: {str(e)}", file=sys.stderr)
                    break
                except json.JSONDecodeError as e:
                    print(f"Failed to parse API response: {str(e)}", file=sys.stderr)
                    break
            
            # Apply offset if specified
            if offset > 0:
                all_tasks = all_tasks[offset:]
            
            return all_tasks[:limit]
            
        except Exception as e:
            print(f"Failed to list tasks: {str(e)}", file=sys.stderr)
            return [] 