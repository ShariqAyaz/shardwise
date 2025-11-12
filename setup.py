"""
ShardWise Setup
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="shardwise",
    version="1.0.0",
    author="ShardWise Contributors",
    description="A comprehensive data preprocessing and annotation pipeline for LLM training",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "shardwise-pipeline=workflows.main_pipeline:main",
            "shardwise-extract=scripts.extract_text:main",
            "shardwise-clean=scripts.clean_text:main",
            "shardwise-chunk=scripts.chunk_text:main",
            "shardwise-dedup=scripts.dedup_filter:main",
            "shardwise-shard=scripts.create_shards:main",
            "shardwise-export=scripts.export_annotation:main",
            "shardwise-labelstudio=scripts.labelstudio_setup:main",
        ],
    },
)

