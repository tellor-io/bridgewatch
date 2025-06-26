#!/usr/bin/env python3
"""
Setup script for Bridge Data Collector
"""

from setuptools import setup, find_packages

setup(
    name="bridge-data-collector",
    version="1.0.0",
    description="Unified Bridge Data Collector for Tellor Layer",
    author="Tellor",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "web3>=6.0.0,<7.0.0",
        "requests>=2.28.0",
        "eth-abi>=4.0.0,<5.0.0",
    ],
    entry_points={
        "console_scripts": [
            "bridgewatch=bridge_watcher:main",
        ],
    },
    python_requires=">=3.8",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
) 