#!/usr/bin/env python3
"""
DataComparePro UI — startup script.

Usage:
    python start.py              # default port 5000
    python start.py --port 8080
    python start.py --debug
"""
import argparse
import os
import sys
from pathlib import Path

# ── Ensure edcp framework and UI are importable ─────────────────────────────
_UI_ROOT = Path(__file__).parent
_FW_ROOT = _UI_ROOT.parent / "edcp"   # shipped alongside edcp_ui/
for _p in (str(_UI_ROOT), str(_FW_ROOT)):
    if _p not in sys.path:
        sys.path.insert(0, _p)


def main():
    parser = argparse.ArgumentParser(description="DataComparePro UI Server v3.0.0")
    parser.add_argument("--port",  type=int, default=5000)
    parser.add_argument("--host",  default="0.0.0.0")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    from api.app import app
    print(f"""
╔══════════════════════════════════════════════════╗
║  DataComparePro  v3.0.0  —  Enterprise Edition   ║
╠══════════════════════════════════════════════════╣
║  URL : http://localhost:{args.port:<24}║
║  API : http://localhost:{args.port}/api/v1/health  ║
╚══════════════════════════════════════════════════╝
    """)
    app.run(host=args.host, port=args.port, debug=args.debug, threaded=True)


if __name__ == "__main__":
    main()
