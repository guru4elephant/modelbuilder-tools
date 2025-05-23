# Batch Job Submitter

A command-line tool for submitting JSONL files to modelbuilder batch inference service.

## Features

- Validate JSONL files for correctness
- Automatically split large JSONL files (>50K lines or >300MB)
- Upload files to BOS (Baidu Object Storage)
- Submit batch inference jobs
- Track job status
- Configure credentials via config file or command line

## Installation

```bash
# Clone the repository
git clone <repository_url>
cd modelbuilder-batch

# Install dependencies
pip install -r batch_job_submitter/requirements.txt

# Optional: Install the package
pip install -e .
```

## Configuration

You need to configure your Qianfan access and secret keys before using the tool:

```bash
# Configure via command line
python -m batch_job_submitter config --ak YOUR_ACCESS_KEY --sk YOUR_SECRET_KEY

# Alternative: Create a config file
# ~/.batch_job_submitter.ini
# [qianfan]
# access_key = YOUR_ACCESS_KEY
# secret_key = YOUR_SECRET_KEY
```

## Usage

### Validate JSONL files

```bash
# Basic validation
python -m batch_job_submitter validate your_file.jsonl

# Detailed validation
python -m batch_job_submitter validate your_file.jsonl -v
```

### Submit JSONL files for batch processing

```bash
# Submit a file
python -m batch_job_submitter submit your_file.jsonl

# Submit with custom job name
python -m batch_job_submitter submit your_file.jsonl --job-name "My custom job"

# Submit and wait for completion
python -m batch_job_submitter submit your_file.jsonl --wait

# Submit without auto-splitting
python -m batch_job_submitter submit your_file.jsonl --no-split

# Submit with specific model ID
python -m batch_job_submitter submit your_file.jsonl --model-id "amv-xys3cq1udmud"
```

### Check job status

```bash
python -m batch_job_submitter status YOUR_TASK_ID
```

## Advanced Configuration

You can create a configuration file at `~/.batch_job_submitter.ini`:

```ini
[qianfan]
access_key = YOUR_ACCESS_KEY
secret_key = YOUR_SECRET_KEY
host = qianfan.baidubce.com

[bos]
endpoint = bj.bcebos.com
bucket = copilot-engine-batch-infer

[job]
model_id = amv-xys3cq1udmud
temperature = 0.6
top_p = 0.01
max_output_tokens = 4096
```

## Requirements

- Python 3.6+
- Required packages:
  - qianfan
  - boto3 (if jsonflow is not available) 
