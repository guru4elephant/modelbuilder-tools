#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
JSONL file processor for validating and splitting large JSONL files
"""

import os
import json
import tempfile
from pathlib import Path
from typing import List, Dict, Any, Tuple, Generator, Optional


class JsonlProcessor:
    """JSONL file processor for validating and splitting large files"""
    
    # Constants for file splitting
    MAX_LINES = 50000
    MAX_SIZE_BYTES = 300 * 1024 * 1024  # 300MB
    
    def __init__(self, input_file: str):
        """
        Initialize JSONL processor
        
        Args:
            input_file: Path to JSONL file
        """
        self.input_file = input_file
        self.input_path = Path(input_file)
        
        if not self.input_path.exists():
            raise FileNotFoundError(f"File not found: {input_file}")
            
    def validate(self, verbose: bool = False) -> Tuple[int, int]:
        """
        Validate JSONL file, checking each line is valid JSON
        
        Args:
            verbose: Print detailed information about invalid lines
            
        Returns:
            Tuple of (valid_count, invalid_count)
        """
        valid_count = 0
        invalid_count = 0
        
        with open(self.input_file, 'r', encoding='utf-8') as f:
            for line_number, line in enumerate(f, 1):
                line = line.strip()
                
                # Skip empty lines
                if not line:
                    invalid_count += 1
                    if verbose:
                        print(f"Line {line_number}: Empty line")
                    continue
                
                # Check if valid JSON
                try:
                    json.loads(line)
                    valid_count += 1
                except json.JSONDecodeError:
                    invalid_count += 1
                    if verbose:
                        print(f"Line {line_number}: Invalid JSON - {line[:50]}...")
        
        if verbose:
            print(f"Total lines: {valid_count + invalid_count}")
            print(f"Valid JSON lines: {valid_count}")
            print(f"Invalid JSON lines: {invalid_count}")
            
        return valid_count, invalid_count
        
    def needs_splitting(self) -> Tuple[bool, str]:
        """
        Check if file needs splitting based on size or line count
        
        Returns:
            Tuple of (needs_splitting, reason)
        """
        # Check file size
        file_size = self.input_path.stat().st_size
        if file_size > self.MAX_SIZE_BYTES:
            return True, f"File size ({file_size / 1024 / 1024:.2f}MB) exceeds limit ({self.MAX_SIZE_BYTES / 1024 / 1024:.2f}MB)"
            
        # Count lines
        line_count = sum(1 for _ in open(self.input_file, 'r', encoding='utf-8'))
        if line_count > self.MAX_LINES:
            return True, f"Line count ({line_count}) exceeds limit ({self.MAX_LINES})"
            
        return False, ""
        
    def split(self, output_dir: Optional[str] = None) -> List[str]:
        """
        Split JSONL file into smaller chunks
        
        Args:
            output_dir: Directory to save split files (default: same as input file)
            
        Returns:
            List of output file paths
        """
        # Determine output directory
        if output_dir:
            out_dir = Path(output_dir)
            out_dir.mkdir(parents=True, exist_ok=True)
        else:
            out_dir = self.input_path.parent
            
        # Base name for output files
        base_name = self.input_path.stem
        
        # Determine chunk size (lines)
        # We'll split by lines since it's simpler and more efficient
        file_size = self.input_path.stat().st_size
        line_count = sum(1 for _ in open(self.input_file, 'r', encoding='utf-8'))
        
        # Use the more restrictive constraint to determine chunk count
        size_based_chunks = (file_size // self.MAX_SIZE_BYTES) + 1
        line_based_chunks = (line_count // self.MAX_LINES) + 1
        chunk_count = max(size_based_chunks, line_based_chunks)
        
        # Calculate lines per chunk
        lines_per_chunk = line_count // chunk_count + 1
        
        # Split file
        output_files = []
        with open(self.input_file, 'r', encoding='utf-8') as f:
            chunk_index = 0
            line_index = 0
            current_chunk = []
            
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                current_chunk.append(line)
                line_index += 1
                
                if line_index >= lines_per_chunk:
                    # Write chunk to file
                    output_file = out_dir / f"{base_name}_{chunk_index + 1}.jsonl"
                    with open(output_file, 'w', encoding='utf-8') as out_f:
                        out_f.write("\n".join(current_chunk))
                    
                    output_files.append(str(output_file))
                    chunk_index += 1
                    line_index = 0
                    current_chunk = []
            
            # Write remaining lines
            if current_chunk:
                output_file = out_dir / f"{base_name}_{chunk_index + 1}.jsonl"
                with open(output_file, 'w', encoding='utf-8') as out_f:
                    out_f.write("\n".join(current_chunk))
                output_files.append(str(output_file))
                
        return output_files 