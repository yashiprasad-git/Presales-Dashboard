#!/usr/bin/env python3
"""
Quick test: load a media plan with the same logic as the pipeline.

Setup (pick one):
  export GOOGLE_APPLICATION_CREDENTIALS=/absolute/path/to/key.json

Or in .streamlit/secrets.toml:
  GOOGLE_APPLICATION_CREDENTIALS = "/path/to/key.json"
  # or paste full JSON as GOOGLE_SERVICE_ACCOUNT_JSON = '''{ ... }'''

Usage:
  python3 test_google_service_account.py "https://docs.google.com/spreadsheets/d/SPREADSHEET_ID/..."
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from pipeline import extract_sheet_id, find_context_tab, read_public_sheet  # noqa: E402


def main() -> None:
    if len(sys.argv) < 2:
        print(__doc__)
        raise SystemExit(1)
    url = sys.argv[1].strip()
    sid = extract_sheet_id(url)
    print(f"File ID: {sid}")
    print("Downloading (service account used if configured)...")
    xls = read_public_sheet(url)
    print("Sheets:", xls.sheet_names)
    tab = find_context_tab(xls)
    print("Context tab:", tab or "(none — check sheet names)")
    print("OK — service account / public access works for this URL.")


if __name__ == "__main__":
    main()
