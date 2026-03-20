# -*- coding: utf-8 -*-
"""
setup.py – pip-installable package definition.

Install (editable development mode):
    pip install -e .

Install from wheel:
    pip install dist/data_compare-3.0.0-py3-none-any.whl

Build wheel:
    pip install build
    python -m build
"""

from setuptools import find_packages, setup

setup(
    name="data_compare",
    version="3.0.0",
    description=(
        "Enterprise Capability-Based Prod vs Dev Data Comparison Framework"
    ),
    long_description=open("README.md", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    python_requires=">=3.8",
    packages=find_packages(
        exclude=["tests*", "*.tests", "*.tests.*"]
    ),
    install_requires=[
        "pandas>=1.3.0",
        "openpyxl>=3.0.0",
    ],
    extras_require={
        "yaml":    ["PyYAML>=5.4"],
        "win32":   ["pywin32>=300"],
        "full":    ["PyYAML>=5.4", "pywin32>=300"],
        "pyspark": ["pyspark>=3.0.0"],
    },
    entry_points={
        "console_scripts": [
            "data-compare=data_compare.main:main",
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries",
        "Topic :: Office/Business :: Financial",
    ],
)
