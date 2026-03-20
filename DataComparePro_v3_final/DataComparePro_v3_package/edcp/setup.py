# -*- coding: utf-8 -*-
"""
setup.py — compatibility shim for edcp / DataComparePro.

All authoritative metadata is in pyproject.toml.
This file exists only for tooling that does not yet read pyproject.toml
(e.g. legacy `pip install -e .` on older pip versions).
"""
from setuptools import setup

# All metadata lives in pyproject.toml — setuptools reads it automatically.
# Keeping this file empty of duplicate metadata prevents drift.
setup()
