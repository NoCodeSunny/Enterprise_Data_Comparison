# DataComparePro v3.0 — Enterprise Data Comparison Platform (EDCP)

## Package Contents

```
DataComparePro_v3/
├── edcp/                          ← Core Python packages
│   ├── edcp/                      ← Enterprise orchestration layer
│   │   ├── batch/                 ← BatchManager, QueueController, RecoveryManager, EngineSelector
│   │   ├── validation/            ← PreFlightValidator
│   │   ├── audit/                 ← EnterpriseAuditLogger
│   │   ├── jobs/                  ← ComparisonJob
│   │   ├── loaders/               ← file_loader, encoding
│   │   └── comparator/            ← tolerance
│   └── data_compare/              ← 9 capability framework
│       ├── capabilities/          ← comparison, tolerance, schema, duplicate, data_quality,
│       │                            parquet, audit, alerts, plugins
│       ├── comparator/            ← schema, duplicate, tolerance
│       ├── config/                ← config_loader (env-var driven, no hardcoded paths)
│       ├── engines/               ← PandasEngine, SparkEngine
│       ├── jobs/                  ← ComparisonJob
│       ├── loaders/               ← file_loader, encoding
│       ├── reporting/             ← ExcelReport, HTMLReport, JSONAudit, ReportBuilder
│       └── registry/              ← CapabilityRegistry
│
├── edcp_ui/                       ← Flask Web API + SPA
│   ├── api/
│   │   ├── app.py                 ← Flask application
│   │   └── v1/batch_api.py        ← 14 REST endpoints (incl. /validate, /report/{type})
│   ├── static/index.html          ← Single-page application (all 10 fixes applied)
│   ├── start.py                   ← Server entry point
│   └── tests/
│       ├── test_api.py            ← 15 legacy API tests
│       ├── test_batch_mvp1.py     ← 84 batch component tests
│       ├── test_comprehensive_v2.py ← 141 unit/integration/API/E2E/UAT tests
│       └── test_extended_v3.py    ← 25 stability/concurrency/chaos/security/observability tests
│
└── framework_tests/               ← 282 framework capability tests
    ├── test_comparison.py
    ├── test_loader.py
    ├── test_parquet.py
    ├── test_tolerance.py
    ├── test_duplicates.py
    ├── test_schema.py
    ├── test_performance.py
    ├── test_edge_cases.py
    ├── test_concurrency.py
    ├── test_failure_recovery.py
    ├── test_debugger.py
    ├── test_key_edge_cases.py
    ├── test_security.py
    ├── test_data_types.py
    ├── test_null_handling.py
    ├── test_config_edge_cases.py
    ├── test_reporting.py
    ├── test_reporting_deep.py
    └── test_cli.py
```

## Installation

```bash
pip install flask pandas openpyxl pyyaml psutil --break-system-packages
```

## Quick Start — CLI

```bash
# Run comparison from YAML config (no InputSheet needed)
cd edcp_ui
PYTHONPATH=../edcp:. python3 start.py   # start the UI

# Or use CLI directly
cd edcp
PYTHONPATH=. python3 -m edcp run config/config.yaml
PYTHONPATH=. python3 -m edcp list-caps
PYTHONPATH=. python3 -m edcp version
```

### Example YAML config (batches: mode)

```yaml
report_root: /path/to/reports
batches:
  - prod_path: /path/to/prod.csv
    dev_path:  /path/to/dev.csv
    result_name: my_comparison
    keys: [ID]
```

## Running Tests

```bash
# Framework capability tests (282)
cd DataComparePro_v3
PYTHONPATH=edcp python3 -m unittest discover -s framework_tests
# Expected: Ran 282 tests — OK (skipped=1)

# UI + Batch + Comprehensive + Extended tests (265)
cd edcp_ui
PYTHONPATH=../edcp:. python3 -m unittest tests.test_api tests.test_batch_mvp1 \
  tests.test_comprehensive_v2 tests.test_extended_v3
# Expected: Ran 265 tests — OK

# TOTAL: 547 tests, 0 failures, 3 expected skips
```

## Starting the Server

```bash
cd edcp_ui
EDCP_REPORT_ROOT=/path/to/reports \
EDCP_INPUT_SHEET=/path/to/InputSheet.xlsx \
python3 start.py
# Open http://localhost:5000
```

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `EDCP_REPORT_ROOT` | `~/DataComparePro/Reports` | Output directory for comparison reports |
| `EDCP_INPUT_SHEET` | `~/DataComparePro/InputSheet_Data_Comparison.xlsx` | InputSheet path |
| `EDCP_EMAIL_TO` | `` | Email recipient for alerts |

## What Was Fixed (v2 → v3)

| Issue | Severity | Fix |
|---|---|---|
| WARN-1: Ghost batches from validate | Warning | New `POST /api/v1/batch/validate` dry-run endpoint |
| WARN-2: Progress no batch selector | Warning | `<select id="PBSEL">` + `loadProgBatchList()` |
| WARN-3: CANCELLING never → CANCELLED | Warning | `_execute_batch` finally checks `cancel_event` |
| MED-1: Job IDs not globally unique | Medium | `BATCH_..._JOB_001` format |
| MED-2: Batch state lost on restart | Medium | SQLite persistence via `_init_db()` |
| MINOR: Hardcoded mtyagi paths | Minor | `_make_defaults()` reads env vars |
| MINOR: Dropdown missing 9,11,13,14 | Minor | All 1–20 values present |
| MINOR: Batch report download broken | Minor | `GET /api/v1/batch/{id}/report/{type}` |
| MINOR: ETA hardcoded 45s | Minor | `loadAvgJobTime()` history-based |
| MINOR: Log modal not batch-scoped | Minor | `openLM(jid, title, batchId)` |

## Test Summary

| Suite | Tests | Result |
|---|---|---|
| Framework Core | 282 | ✅ PASS (1 skip — pyarrow full install) |
| Legacy API | 15 | ✅ PASS |
| Batch MVP1 | 84 | ✅ PASS |
| Comprehensive v2 | 141 | ✅ PASS (2 skips — capacity isolation) |
| Extended v3 | 25 | ✅ PASS |
| **TOTAL** | **547** | **✅ 0 FAILURES** |
