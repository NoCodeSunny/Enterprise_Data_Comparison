#!/usr/bin/env python3
"""
data_compare UI – startup script.

Usage:
    python start.py            # default port 5000
    python start.py --port 8080
    python start.py --debug
"""
import argparse, os, sys
from pathlib import Path

# Ensure framework is importable
FW = Path(__file__).parent.parent / "data_compare_framework"
if str(FW) not in sys.path:
    sys.path.insert(0, str(FW))
UI = Path(__file__).parent
sys.path.insert(0, str(UI))

def main():
    parser = argparse.ArgumentParser(description="data_compare UI Server")
    parser.add_argument("--port",  type=int, default=5000)
    parser.add_argument("--host",  default="0.0.0.0")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    from api.app import app
    print(f"""
╔══════════════════════════════════════════════╗
║  data_compare UI  v3.2  –  Enterprise Edition ║
╠══════════════════════════════════════════════╣
║  URL : http://localhost:{args.port:<20}║
║  API : http://localhost:{args.port}/api/health  ║
╚══════════════════════════════════════════════╝
    """)
    app.run(host=args.host, port=args.port, debug=args.debug, threaded=True)

if __name__ == "__main__":
    main()
