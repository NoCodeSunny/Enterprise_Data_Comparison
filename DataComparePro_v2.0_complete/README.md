# data_compare – Enterprise Capability-Based Data Comparison Framework v3.0

## Architecture Overview

```
data_compare_framework/
│
├── data_compare/                        # Installable package root
│   ├── __init__.py                      # Public API: run_comparison()
│   ├── orchestrator.py                  # Pipeline controller
│   ├── main.py                          # CLI entry point
│   │
│   ├── context/
│   │   └── run_context.py               # Shared context factory (69-key schema)
│   │
│   ├── capabilities/                    # Each capability is independent
│   │   ├── base.py                      # BaseCapability abstract class
│   │   ├── comparison/
│   │   │   └── comparison_capability.py # Core value comparison engine
│   │   ├── tolerance/
│   │   │   └── tolerance_capability.py  # Numerical rounding rules
│   │   ├── duplicate/
│   │   │   └── duplicate_capability.py  # Duplicate detection + _SEQ_ alignment
│   │   ├── schema/
│   │   │   └── schema_capability.py     # Column schema diff detection
│   │   ├── data_quality/
│   │   │   └── data_quality_capability.py # Null rates, distinct counts, type check
│   │   ├── audit/
│   │   │   └── audit_capability.py      # Audit trail + capability timings
│   │   ├── alerts/
│   │   │   └── alerts_capability.py     # Threshold-based alert rules
│   │   └── plugins/
│   │       └── plugin_capability.py     # Dynamic user-defined plugins
│   │
│   ├── registry/
│   │   └── capability_registry.py       # Dynamic registration + run_pipeline()
│   │
│   ├── comparator/                      # Business logic (unchanged from v2)
│   │   ├── core.py
│   │   ├── tolerance.py
│   │   ├── duplicate.py
│   │   └── schema.py
│   │
│   ├── loaders/                         # File ingestion (CSV/TXT/XLSX)
│   │   ├── encoding.py
│   │   └── file_loader.py
│   │
│   ├── reporting/                       # Output generation
│   │   ├── excel_report.py              # Single-pass openpyxl writer
│   │   ├── html_report.py               # HTML summary + email dispatch
│   │   ├── json_audit.py                # JSON sidecar for CI/CD
│   │   └── report_builder.py            # Assembles workbook from context
│   │
│   ├── utils/
│   │   ├── logger.py                    # Centralised logging
│   │   ├── helpers.py                   # trim_df, sanitise_filename, etc.
│   │   └── validation.py                # InputSheet config validation
│   │
│   └── config/
│       └── config_loader.py             # YAML loader with hardcoded defaults
│
├── config/
│   └── config.yaml                      # Example configuration file
│
├── run.py                               # Top-level runner
├── setup.py                             # pip-installable package
└── requirements.txt
```

---

## Quick Start

### Local run (hardcoded defaults)
```bash
pip install -e .
python run.py
```

### Local run with YAML config
```bash
python run.py --config config/config.yaml
```

### Run only specific capabilities
```bash
python run.py --config config/config.yaml --capabilities comparison,schema,data_quality
```

### List all capabilities
```bash
python run.py --list-capabilities
```

### CI/CD mode (exit 1 on any failure)
```bash
python run.py --config config/config.yaml --fail-on-error
```

---

## Library Usage

### Full pipeline
```python
from data_compare.orchestrator import run_comparison

summaries = run_comparison("config/config.yaml")
```

### Run only selected capabilities
```python
from pathlib import Path
from data_compare.context.run_context import make_context
from data_compare.registry.capability_registry import CapabilityRegistry
from data_compare.reporting.report_builder import build_report

registry = CapabilityRegistry()

context = make_context(
    prod_csv_path=Path("data/prod.csv"),
    dev_csv_path=Path("data/dev.csv"),
    prod_name="prod.csv",
    dev_name="dev.csv",
    result_name="MyComparison",
    report_root=Path("reports/"),
    keys=["TradeID", "Portfolio"],
    ignore_fields=["LoadTimestamp", "BatchID"],
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
report_path = build_report(context)
print(f"Report: {report_path}")
print(f"Failures: {context['results']['matched_failed']}")
```

### Run only the data quality module
```python
from pathlib import Path
from data_compare.context.run_context import make_context
from data_compare.capabilities.data_quality.data_quality_capability import DataQualityCapability
from data_compare.loaders.encoding import read_csv_robust
from data_compare.utils.helpers import trim_df

context = make_context(capabilities_cfg={"data_quality": True})
context["prod_df"] = trim_df(read_csv_robust(Path("prod.csv"), dtype=str))
context["dev_df"]  = trim_df(read_csv_robust(Path("dev.csv"),  dtype=str))

dq = DataQualityCapability()
context = dq.run(context)

report = context["metrics"]["data_quality_report"]
print(report.to_string())
```

### Run only the comparison module (no deduplication, no schema, no DQ)
```python
from pathlib import Path
from data_compare.context.run_context import make_context
from data_compare.capabilities.comparison.comparison_capability import ComparisonCapability
from data_compare.reporting.report_builder import build_report

context = make_context(
    prod_csv_path=Path("prod.csv"),
    dev_csv_path=Path("dev.csv"),
    prod_name="prod.csv",
    dev_name="dev.csv",
    result_name="QuickCheck",
    report_root=Path("reports/"),
    keys=["ID"],
    capabilities_cfg={
        "comparison":   True,
        "tolerance":    False,
        "duplicate":    False,
        "schema":       False,
        "data_quality": False,
        "audit":        False,
        "alerts":       False,
        "plugins":      False,
    },
)

context = ComparisonCapability().run(context)
build_report(context)
```

---

## PySpark Usage

```python
# Step 1: Build the zip
import zipfile, os
with zipfile.ZipFile("data_compare.zip", "w", zipfile.ZIP_DEFLATED) as zf:
    for root, dirs, files in os.walk("data_compare"):
        for fname in files:
            if fname.endswith(".py"):
                fpath = os.path.join(root, fname)
                zf.write(fpath)

# Step 2: Register on Spark
spark.sparkContext.addPyFile("/dbfs/mnt/shared/data_compare.zip")

# Step 3: Run
from data_compare.orchestrator import run_comparison
summaries = run_comparison("/dbfs/mnt/config/config.yaml")
```

---

## Adding a Custom Capability

```python
# my_project/capabilities/custom_check.py
from data_compare.capabilities.base import BaseCapability

class CustomCheckCapability(BaseCapability):
    NAME = "custom_check"

    def execute(self, context):
        prod_df = context.get("prod_df")
        # ... your logic ...
        context["plugin_outputs"]["custom_check"] = {"status": "ok"}
        return context
```

```python
# Register at runtime (no existing files modified)
from data_compare.registry.capability_registry import default_registry
from my_project.capabilities.custom_check import CustomCheckCapability

default_registry.register(CustomCheckCapability())
```

```yaml
# config.yaml – enable it
capabilities:
  custom_check: true
```

---

## Capability Execution Order

```
tolerance → schema → duplicate → comparison → data_quality → audit → alerts → plugins → [custom]
```

Each capability only reads what it needs from context and writes its own keys.
No capability imports from another capability.

---

## Output Files

| File | Description |
|---|---|
| `{ResultName}_RecordComparison.xlsx` | Per-batch workbook with all sheets |
| `Final_Comparison_Summary.html` | Rich HTML summary email body |
| `run_audit.json` | Machine-readable CI/CD audit log |

### Excel Sheets Written

| Sheet | Capability | Always present |
|---|---|---|
| Grouped Differences | comparison | Yes |
| Count Differences | comparison | Yes |
| Pass Comparison | comparison | Yes |
| Duplicates Records | duplicate | Yes |
| Schema Differences | schema | Yes |
| Settings | report_builder | Yes |
| Data Quality Report | data_quality | When enabled |
| Audit Trail | audit | When enabled |
| Audit Summary | audit | When enabled |
| Cap Timings | audit | When enabled |
| Alerts | alerts | When triggered |

---

## Feature Inventory (all 69 features from v2.0 retained)

All features [F-01] through [F-69] from the enterprise v2.0 script are
preserved without any business logic change. The refactor is purely
structural: code moved to modules and wrapped in capability classes.

Additional capabilities added in v3.0:
- **DataQualityCapability** – null rates, distinct counts, type mismatches, numeric stats
- **AuditCapability** – event trail, per-capability timings, run ID
- **AlertsCapability** – configurable threshold rules with ERROR/WARNING/INFO levels
- **PluginCapability** – dynamic user-defined plugin modules
- **CapabilityRegistry** – dynamic registration, ordered pipeline, enable/disable per batch
- **Context object** – single shared data bus, no global state
