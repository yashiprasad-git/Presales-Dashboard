"""
Thin wrapper — runs the MDB updater from the `mdb/` package.
Use: python db_updater.py [--since YYYY-MM-DD] [--retry-blocked]
Or:   cd mdb && python db_updater.py monday_config.json ...
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
_MDB = _ROOT / "mdb"
_CFG = _MDB / "monday_config.json"


def main() -> None:
    # Default: mdb/monday_config.json + extra args (--since, --retry-blocked, etc.)
    rest = sys.argv[1:]
    if rest and rest[0].endswith(".json") and not rest[0].startswith("-"):
        cfg = (_ROOT / rest[0]) if (_ROOT / rest[0]).is_file() else (_MDB / rest[0])
        extra = rest[1:]
    else:
        cfg = _CFG
        extra = rest
    cmd = [sys.executable, str(_MDB / "db_updater.py"), str(cfg)] + extra
    raise SystemExit(subprocess.call(cmd, cwd=str(_MDB)))


if __name__ == "__main__":
    main()
