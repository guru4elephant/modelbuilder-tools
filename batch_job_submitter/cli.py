#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Command line interface for batch job submitter
"""

import os
import sys
import time
import argparse
from pathlib import Path
from typing import List, Dict, Any, Optional

from batch_job_submitter.config import Config
from batch_job_submitter.jsonl_processor import JsonlProcessor
from batch_job_submitter.job_submitter import JobSubmitter


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Submit JSONL files to modelbuilder batch inference service",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # Create subparsers for different commands
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # Config command
    config_parser = subparsers.add_parser("config", help="Configure the CLI")
    config_parser.add_argument("--ak", "--access-key", dest="access_key", help="Qianfan access key")
    config_parser.add_argument("--sk", "--secret-key", dest="secret_key", help="Qianfan secret key")
    config_parser.add_argument("--config-file", help="Path to configuration file")
    
    # Submit command
    submit_parser = subparsers.add_parser("submit", help="Submit a JSONL file for batch processing")
    submit_parser.add_argument("file", help="JSONL file to submit")
    submit_parser.add_argument("--job-name", help="Custom job name")
    submit_parser.add_argument("--description", help="Job description")
    submit_parser.add_argument("--model-id", help="Model ID to use for inference")
    submit_parser.add_argument("--no-split", action="store_true", help="Don't split large files")
    submit_parser.add_argument("--output-dir", help="Directory for split files")
    submit_parser.add_argument("--wait", action="store_true", help="Wait for job to complete")
    
    # Status command
    status_parser = subparsers.add_parser("status", help="Check job status")
    status_parser.add_argument("task_id", help="Task ID to check")
    
    # List command
    list_parser = subparsers.add_parser("list", help="List batch inference tasks")
    list_parser.add_argument("--limit", type=int, default=20, help="Maximum number of tasks to return")
    list_parser.add_argument("--offset", type=int, default=0, help="Offset for pagination")
    list_parser.add_argument("--status", nargs="*", choices=["Done", "Running", "Failed", "Cancelled"], 
                           help="Filter by task status (can specify multiple)")
    list_parser.add_argument("--all-status", action="store_true", 
                           help="Show tasks with all statuses (default: Done, Running, Failed, Cancelled)")
    
    # Validate command
    validate_parser = subparsers.add_parser("validate", help="Validate a JSONL file")
    validate_parser.add_argument("file", help="JSONL file to validate")
    validate_parser.add_argument("-v", "--verbose", action="store_true", help="Show detailed validation information")
    
    return parser.parse_args()


def handle_config(args, config: Config):
    """Handle config command"""
    # Update configuration with provided values
    if args.access_key:
        config.set("qianfan", "access_key", args.access_key)
        
    if args.secret_key:
        config.set("qianfan", "secret_key", args.secret_key)
        
    # Save configuration
    config.save_config()
    
    print(f"Configuration saved to {config.config_file}")
    

def handle_submit(args, config: Config):
    """Handle submit command"""
    # Check file exists
    if not Path(args.file).exists():
        print(f"Error: File '{args.file}' not found", file=sys.stderr)
        return 1
        
    # Update model ID if provided
    if args.model_id:
        config.set("job", "model_id", args.model_id)
        
    # Create job submitter
    job_submitter = JobSubmitter(config)
    
    # Create JSONL processor
    jsonl_processor = JsonlProcessor(args.file)
    
    # Validate JSONL file
    valid_count, invalid_count = jsonl_processor.validate()
    print(f"JSONL validation: {valid_count} valid, {invalid_count} invalid lines")
    
    if invalid_count > 0:
        print("Warning: File contains invalid JSON lines which may cause issues", file=sys.stderr)
        response = input("Continue anyway? (y/N): ")
        if response.lower() != 'y':
            return 1
    
    # Check if file needs splitting
    needs_split, reason = jsonl_processor.needs_splitting()
    
    if needs_split and not args.no_split:
        print(f"File needs splitting: {reason}")
        
        # Split file
        output_dir = args.output_dir or str(Path(args.file).parent / "splits")
        print(f"Splitting file into chunks in {output_dir}...")
        
        split_files = jsonl_processor.split(output_dir)
        print(f"File split into {len(split_files)} chunks")
        
        # Submit each chunk
        task_ids = []
        for i, file_path in enumerate(split_files):
            print(f"Uploading chunk {i+1}/{len(split_files)}: {file_path}")
            success, remote_url = job_submitter.upload_file(file_path)
            
            if not success:
                print(f"Failed to upload file: {file_path}", file=sys.stderr)
                continue
                
            print(f"File uploaded to {remote_url}")
            
            # Generate job name with chunk number
            job_name = args.job_name or f"batch_job_{Path(args.file).stem}_chunk{i+1}"
            
            # Submit job
            print(f"Submitting job: {job_name}")
            success, task_id = job_submitter.submit_job(
                bos_uri=remote_url,
                job_name=job_name,
                description=args.description
            )
            
            if not success:
                print(f"Failed to submit job for file: {file_path}", file=sys.stderr)
                continue
                
            print(f"Job submitted with task ID: {task_id}")
            task_ids.append(task_id)
            
        print(f"All chunks submitted. Task IDs: {', '.join(task_ids)}")
        
        # Wait for jobs to complete if requested
        if args.wait and task_ids:
            wait_for_jobs(job_submitter, task_ids)
    else:
        # No splitting needed or explicitly disabled
        if needs_split and args.no_split:
            print("Warning: File exceeds recommended size limits but splitting is disabled", file=sys.stderr)
            
        # Upload file
        print(f"Uploading file: {args.file}")
        success, remote_url = job_submitter.upload_file(args.file)
        
        if not success:
            print(f"Failed to upload file: {args.file}", file=sys.stderr)
            return 1
            
        print(f"File uploaded to {remote_url}")
        
        # Submit job
        job_name = args.job_name or f"batch_job_{Path(args.file).stem}"
        print(f"Submitting job: {job_name}")
        
        success, task_id = job_submitter.submit_job(
            bos_uri=remote_url,
            job_name=job_name,
            description=args.description
        )
        
        if not success:
            print(f"Failed to submit job", file=sys.stderr)
            return 1
            
        print(f"Job submitted with task ID: {task_id}")
        
        # Wait for job to complete if requested
        if args.wait:
            wait_for_jobs(job_submitter, [task_id])
            
    return 0


def handle_status(args, config: Config):
    """Handle status command"""
    # Create job submitter
    job_submitter = JobSubmitter(config)
    
    # Check task status
    print(f"Checking status for task: {args.task_id}")
    status = job_submitter.check_task_status(args.task_id)
    
    if not status:
        print("Failed to get task status", file=sys.stderr)
        return 1
        
    # Print status information
    print(f"Task ID: {status.get('taskId', 'Unknown')}")
    print(f"Name: {status.get('name', 'Unknown')}")
    print(f"Status: {status.get('status', 'Unknown')}")
    print(f"Create time: {status.get('createTime', 'Unknown')}")
    
    if 'startTime' in status:
        print(f"Start time: {status['startTime']}")
        
    if 'endTime' in status:
        print(f"End time: {status['endTime']}")
        
    print(f"Progress: {status.get('progress', 0)}%")
    
    return 0


def handle_list(args, config: Config):
    """Handle list command"""
    # Create job submitter
    job_submitter = JobSubmitter(config)
    
    # Determine status filter
    run_status = None
    if args.status:
        run_status = args.status
    elif not args.all_status:
        # Default to common statuses if not specified
        run_status = ["Done", "Running", "Failed", "Cancelled"]
    
    # Get task list
    status_info = f" (status: {', '.join(run_status)})" if run_status else " (all statuses)"
    print(f"Fetching batch inference tasks (limit: {args.limit}, offset: {args.offset}){status_info}...")
    
    tasks = job_submitter.list_tasks(limit=args.limit, offset=args.offset, run_status=run_status)
    
    if not tasks:
        print("No tasks found or failed to fetch tasks")
        return 0
        
    # Print task list in a table format
    print(f"\nFound {len(tasks)} tasks:")
    print("-" * 120)
    print(f"{'Task ID':<20} {'Name':<30} {'Status':<12} {'Model ID':<20} {'Create Time':<20}")
    print("-" * 120)
    
    for task in tasks:
        task_id = task.get('taskId', 'Unknown')
        task_id = (task_id[:18] + '...' if task_id and len(task_id) > 20 else task_id) if task_id else 'Unknown'
        
        name = task.get('name', 'Unknown')
        name = (name[:28] + '...' if name and len(name) > 30 else name) if name else 'Unknown'
        
        status = task.get('status', 'Unknown') or 'Unknown'
        
        model_id = task.get('modelId', 'Unknown')
        model_id = (model_id[:18] + '...' if model_id and len(model_id) > 20 else model_id) if model_id else 'Unknown'
        
        create_time = task.get('createTime', 'Unknown')
        create_time = (create_time[:18] if create_time else 'Unknown') if create_time else 'Unknown'
        
        print(f"{task_id:<20} {name:<30} {status:<12} {model_id:<20} {create_time:<20}")
    
    print("-" * 120)
    print(f"Use 'batch-submit status <task_id>' to get detailed information about a specific task")
    
    return 0


def handle_validate(args, config: Config):
    """Handle validate command"""
    # Check file exists
    if not Path(args.file).exists():
        print(f"Error: File '{args.file}' not found", file=sys.stderr)
        return 1
        
    # Create JSONL processor
    jsonl_processor = JsonlProcessor(args.file)
    
    # Validate JSONL file
    print(f"Validating JSONL file: {args.file}")
    valid_count, invalid_count = jsonl_processor.validate(verbose=args.verbose)
    
    print(f"Validation complete: {valid_count} valid, {invalid_count} invalid lines")
    
    # Check if file needs splitting
    needs_split, reason = jsonl_processor.needs_splitting()
    if needs_split:
        print(f"Note: File needs splitting for batch submission: {reason}")
        
    return 0 if invalid_count == 0 else 1


def wait_for_jobs(job_submitter: JobSubmitter, task_ids: List[str]):
    """Wait for jobs to complete"""
    pending_tasks = set(task_ids)
    completed_tasks = set()
    
    print("Waiting for jobs to complete...")
    
    try:
        while pending_tasks:
            for task_id in list(pending_tasks):
                status = job_submitter.check_task_status(task_id)
                
                if not status:
                    # Failed to get status, assume still pending
                    continue
                    
                task_status = status.get('status', 'Unknown')
                task_progress = status.get('progress', 0)
                
                print(f"Task {task_id}: {task_status} ({task_progress}%)")
                
                if task_status in ['SUCCESS', 'FAILED', 'CANCELED']:
                    pending_tasks.remove(task_id)
                    completed_tasks.add(task_id)
                    
            if pending_tasks:
                # Sleep before checking again
                time.sleep(30)
                
        print("All jobs completed")
    except KeyboardInterrupt:
        print("\nStopped waiting for jobs")
        

def main():
    """Main entry point"""
    # Parse arguments
    args = parse_args()
    
    # Create configuration
    config = Config(args.config_file if hasattr(args, 'config_file') and args.config_file else None)
    
    # Handle command
    if args.command == "config":
        return handle_config(args, config)
    elif args.command == "submit":
        return handle_submit(args, config)
    elif args.command == "status":
        return handle_status(args, config)
    elif args.command == "list":
        return handle_list(args, config)
    elif args.command == "validate":
        return handle_validate(args, config)
    else:
        print("Please specify a command. Use --help for usage information.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main()) 