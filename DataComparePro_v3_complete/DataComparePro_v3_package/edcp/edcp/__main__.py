# -*- coding: utf-8 -*-
"""
Allow running the framework as:
    python -m data_compare run config.yaml
    python -m data_compare validate config.yaml
    python -m data_compare list-caps
    python -m data_compare version
"""
from data_compare.cli import main
main()
