# -*- coding: utf-8 -*-
"""
run.py – Top-level runner.

Local usage
-----------
    python run.py
    python run.py --config config/config.yaml
    python run.py --capabilities comparison,data_quality

PySpark usage
-------------
    # Step 1 – zip the package from the edcp/ directory
    import zipfile, os
    with zipfile.ZipFile("data_compare.zip", "w", zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, files in os.walk("data_compare"):
            for fname in files:
                if fname.endswith(".py"):
                    fpath = os.path.join(root, fname)
                    zf.write(fpath)

    # Step 2 – upload and register on Spark
    spark.sparkContext.addPyFile("/dbfs/mnt/shared/data_compare.zip")

    # Step 3 – run
    from data_compare.orchestrator import run_comparison
    summaries = run_comparison("/dbfs/mnt/config/config.yaml")

    # Or run only selected capabilities
    from data_compare.context.run_context import make_context
    from data_compare.registry.capability_registry import CapabilityRegistry
    from pathlib import Path

    registry = CapabilityRegistry()
    context  = make_context(
        prod_csv_path=Path("/dbfs/mnt/data/prod.csv"),
        dev_csv_path=Path("/dbfs/mnt/data/dev.csv"),
        prod_name="prod.csv",
        dev_name="dev.csv",
        result_name="spark_run_001",
        report_root=Path("/dbfs/mnt/reports/"),
        keys=["TradeID", "Portfolio"],
        capabilities_cfg={
            "comparison":   True,
            "tolerance":    True,
            "duplicate":    True,
            "schema":       True,
            "data_quality": True,
            "audit":        True,
            "alerts":       False,
            "plugins":      False,
        },
    )
    context = registry.run_pipeline(context)
"""

import sys
import os

# Ensure the framework directory is on the path when run directly
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_compare.main import main  # noqa: E402

if __name__ == "__main__":
    main()
