#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages
import os

# Get the long description from the README file
current_dir = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(current_dir, "README.md"), "r", encoding="utf-8") as fh:
    long_description = fh.read()

# Get requirements
with open(os.path.join(current_dir, "requirements.txt"), "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh.read().splitlines() if line.strip() and not line.startswith('#')]

setup(
    name="modelbuilder-batch-submitter",
    version="0.1.0",
    author="guru4elephant",
    author_email="guru4elephant@gmail.com",
    description="A command-line tool for submitting JSONL files to modelbuilder batch inference service",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/guru4elephant/modelbuilder-tools",
    project_urls={
        "Bug Reports": "https://github.com/guru4elephant/modelbuilder-tools/issues",
        "Source": "https://github.com/guru4elephant/modelbuilder-tools",
    },
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Operating System :: OS Independent",
    ],
    keywords="modelbuilder batch inference jsonl ai ml",
    python_requires=">=3.6",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "batch-submit=batch_job_submitter.cli:main",
            "modelbuilder-batch=batch_job_submitter.cli:main",
        ],
    },
    include_package_data=True,
    zip_safe=False,
) 