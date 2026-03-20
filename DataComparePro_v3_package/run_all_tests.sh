#!/usr/bin/env bash
# DataComparePro v3.0 — Run all 547 tests
set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
EDCP_PATH="$SCRIPT_DIR/edcp"
UI_PATH="$SCRIPT_DIR/edcp_ui"
FRAMEWORK_TESTS="$SCRIPT_DIR/framework_tests"

echo "============================================"
echo " DataComparePro v3.0 — Full Test Suite"
echo "============================================"

echo ""
echo "--- Framework Core Tests (282) ---"
cd "$SCRIPT_DIR"
PYTHONPATH="$EDCP_PATH" python3 -m unittest discover -s framework_tests -v 2>&1 | tail -3

echo ""
echo "--- UI + Batch Tests (265) ---"
cd "$UI_PATH"
PYTHONPATH="$EDCP_PATH:$UI_PATH" python3 -m unittest \
  tests.test_api \
  tests.test_batch_mvp1 \
  tests.test_comprehensive_v2 \
  tests.test_extended_v3 2>&1 | tail -3

echo ""
echo "============================================"
echo " All done. Expected: 547 tests, 0 failures"
echo "============================================"
