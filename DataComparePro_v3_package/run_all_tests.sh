#!/usr/bin/env bash
# DataComparePro v3.0.0 — Run all 617 tests
set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
EDCP_PATH="$SCRIPT_DIR/edcp"
UI_PATH="$SCRIPT_DIR/edcp_ui"

echo "============================================"
echo " DataComparePro v3.0.0 — Full Test Suite"
echo "============================================"

echo ""
echo "--- Framework Core Tests (282) ---"
cd "$SCRIPT_DIR"
PYTHONPATH="$EDCP_PATH" python3 -m unittest discover -s framework_tests 2>&1 | tail -3

echo ""
echo "--- UI + Batch + Comprehensive + Extended + Fix Tests (335) ---"
cd "$UI_PATH"
PYTHONPATH="$EDCP_PATH:$UI_PATH" python3 -m unittest \
  tests.test_api \
  tests.test_batch_mvp1 \
  tests.test_comprehensive_v2 \
  tests.test_extended_v3 \
  tests.test_fixes_v4 2>&1 | tail -3

echo ""
echo "============================================"
echo " Expected: 617 tests, 0 failures, 7 skips"
echo "============================================"
