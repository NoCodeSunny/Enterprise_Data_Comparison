# -*- coding: utf-8 -*-
"""
data_compare – Enterprise Capability-Based Data Comparison Framework  v3.0
===========================================================================

Quick start
-----------
    from data_compare.orchestrator import run_comparison
    run_comparison("config/config.yaml")

Run only selected capabilities
------------------------------
    from data_compare.context.run_context import make_context
    from data_compare.registry.capability_registry import CapabilityRegistry

    registry = CapabilityRegistry()
    context  = make_context(
        prod_csv_path=Path("prod.csv"),
        dev_csv_path=Path("dev.csv"),
        result_name="MyTest",
        report_root=Path("reports/"),
        capabilities_cfg={"comparison": True, "data_quality": True,
                          "schema": False, "duplicate": False,
                          "tolerance": False, "audit": False,
                          "alerts": False, "plugins": False},
    )
    context = registry.run_pipeline(context)

PySpark
-------
    spark.sparkContext.addPyFile("data_compare.zip")
    from data_compare.orchestrator import run_comparison
    run_comparison("/dbfs/mnt/config/config.yaml")
"""

from data_compare.orchestrator import run_comparison

__version__ = "3.0.0"
__all__     = ["run_comparison"]
