#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

with open("batch_job_submitter/README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("batch_job_submitter/requirements.txt", "r", encoding="utf-8") as fh:
    requirements = fh.read().splitlines()

setup(
    name="batch_job_submitter",
    version="0.1.0",
    author="guru4elephant",
    author_email="guru4elephant@gmail.com",
    description="CLI tool for submitting batch jobs to modelbuilder",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/modelbuilder-batch",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "batch-submit=batch_job_submitter.cli:main",
        ],
    },
) 