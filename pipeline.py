#!/usr/bin/env python3
"""
pipeline.py — Unified Presales Pipeline
Replaces monday_presales_pipeline.py + mdb_pipeline.py.

Steps (Supabase is the single store throughout):
  1. Monday.com  → campaigns table   (raw campaign data saved immediately)
  2. Google Sheets → context_rows + access_blocked (media plan context)
  3. OpenAI      → update campaigns  (derived language + recommended category)
  4. Inventory   → update campaigns + alerts (inventory counts + status)

Usage:
  python3 pipeline.py <monday_config.json> <inventory_path> [--since YYYY-MM-DD]

Secrets (env vars or .streamlit/secrets.toml):
  MONDAY_API_KEY
  OPENAI_API_KEY
  DATABASE_URL
"""

import io
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    psycopg2 = None  # type: ignore

try:
    from openai import OpenAI
except ImportError:
    OpenAI = None  # type: ignore


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MONDAY_API_URL  = "https://api.monday.com/v2"
MONDAY_ITEM_URL = "https://silverpush-global.monday.com/boards/{board_id}/pulses/{item_id}"
PRESALES_DIR    = Path(__file__).resolve().parent
FIRST_RUN_SINCE = "2026-03-20"

PRODUCT_MATCH_KEYWORDS = ["mirror", "mirrors", "youtube"]

TACTIC_KEYWORDS    = ["tactic", "tactique", "táctica", "tactiek", "taktik", "戦術"]
SUBTACTIC_KEYWORDS = [
    # English variants
    "sub-tactic", "subtactic", "sub tactic", "subtact",
    # French: "SOUS-TACTIQUES" — "sous-tact" is a prefix match
    "sous-tact", "sous tact",
    # Spanish / Dutch / German
    "sub-táctica", "sub-taktik",
    # Japanese
    "サブタクティック",
]
SIGNAL_KEYWORDS    = [
    "signal", "signals",
    # French
    "signaux",
    # Spanish
    "señal",
    # Dutch
    "signaal",
    # Hindi
    "संकेत",
    # Japanese
    "シグナル",
]

_TACTIC_LANG = {"tactique": "French", "táctica": "Spanish", "tactiek": "Dutch",
                "taktik": "German", "戦術": "Japanese", "tactic": "English"}
_SIGNAL_LANG = {"signaux": "French", "señal": "Spanish", "signaal": "Dutch",
                "संकेत": "Hindi", "シグナル": "Japanese", "signal": "English"}


# ---------------------------------------------------------------------------
# Secrets / DB
# ---------------------------------------------------------------------------

def _load_secrets() -> Dict[str, str]:
    p = PRESALES_DIR / ".streamlit" / "secrets.toml"
    if not p.exists():
        return {}
    try:
        try:
            import tomllib
        except ImportError:
            import tomli as tomllib  # type: ignore
        with open(p, "rb") as f:
            return tomllib.load(f)
    except Exception:
        return {}


def _get_env(key: str) -> str:
    v = os.getenv(key, "").strip()
    return v or _load_secrets().get(key, "").strip()


def get_db():
    if psycopg2 is None:
        raise RuntimeError("psycopg2 not installed. Run: pip3 install psycopg2-binary")
    url = _get_env("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is not set.")
    conn = psycopg2.connect(url)
    conn.autocommit = False
    return conn


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

def init_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
              run_id              TEXT PRIMARY KEY,
              started_at_utc      TEXT NOT NULL,
              finished_at_utc     TEXT,
              status              TEXT NOT NULL,
              stdout              TEXT,
              stderr              TEXT
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS campaigns (
              id                       SERIAL PRIMARY KEY,
              run_id                   TEXT NOT NULL,
              monday_item_id           TEXT UNIQUE NOT NULL,
              monday_board_id          TEXT,
              monday_url               TEXT,
              region                   TEXT,
              campaign_name            TEXT,
              brand_name               TEXT,
              vertical                 TEXT,
              country                  TEXT,
              run_dates                TEXT,
              rfp_summary              TEXT,
              targeting                TEXT,
              trigger_list             TEXT,
              any_other_details        TEXT,
              products_to_pitch        TEXT,
              monday_submitted_at      TEXT,
              derived_language         TEXT,
              recommended_category     TEXT,
              inventory_status         TEXT,
              available_inventory_count INTEGER,
              p1_channel_count         INTEGER,
              p2_channel_count         INTEGER,
              p3_channel_count         INTEGER,
              media_plan_url           TEXT,
              context_status           TEXT,
              recommendation_basis     TEXT,
              error_log                TEXT,
              inserted_at_utc          TEXT NOT NULL,
              updated_at_utc           TEXT
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
              alert_id                  SERIAL PRIMARY KEY,
              monday_item_id            TEXT,
              monday_url                TEXT,
              region                    TEXT,
              campaign_name             TEXT,
              brand_name                TEXT,
              country                   TEXT,
              derived_language          TEXT,
              products_to_pitch         TEXT,
              monday_run_dates          TEXT,
              monday_submitted_at_utc   TEXT,
              recommended_category      TEXT,
              inventory_status          TEXT NOT NULL,
              p1_channel_count          INTEGER,
              p2_channel_count          INTEGER,
              p3_channel_count          INTEGER,
              available_inventory_count INTEGER,
              error_log                 TEXT,
              date_flagged_utc          TEXT NOT NULL,
              resolved_at_utc           TEXT,
              resolved_by               TEXT,
              resolved_note             TEXT
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS context_rows (
              id               SERIAL PRIMARY KEY,
              run_id           TEXT,
              monday_item_id   TEXT,
              monday_board_id  TEXT,
              monday_url       TEXT,
              region           TEXT,
              campaign_name    TEXT,
              brand            TEXT,
              country          TEXT,
              vertical         TEXT,
              brief            TEXT,
              derived_language TEXT,
              local_language   TEXT,
              tactic_en        TEXT,
              subtactic_en     TEXT,
              signal_en        TEXT,
              tactic_local     TEXT,
              subtactic_local  TEXT,
              signal_local     TEXT,
              inserted_at_utc  TEXT NOT NULL
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS access_blocked (
              id               SERIAL PRIMARY KEY,
              run_id           TEXT,
              monday_item_id   TEXT,
              monday_board_id  TEXT,
              monday_url       TEXT,
              region           TEXT,
              campaign_name    TEXT,
              brand            TEXT,
              country          TEXT,
              media_plan_url   TEXT,
              error_message    TEXT,
              date_flagged_utc TEXT NOT NULL,
              resolved_at_utc  TEXT,
              resolved_by      TEXT,
              resolved_note    TEXT
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_campaigns_item ON campaigns(monday_item_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_alerts_open    ON alerts(resolved_at_utc, region);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_blocked_open   ON access_blocked(resolved_at_utc, region);")
        # Migrations: add columns introduced after initial schema
        cur.execute("ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS media_plan_url TEXT;")
        cur.execute("ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS context_status TEXT;")
        cur.execute("ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS recommendation_basis TEXT;")
    conn.commit()


# ---------------------------------------------------------------------------
# Monday.com API
# ---------------------------------------------------------------------------

def _monday_post(api_key: str, query: str, variables: Dict) -> Dict:
    resp = requests.post(
        MONDAY_API_URL,
        json={"query": query, "variables": variables},
        headers={"Authorization": api_key, "Content-Type": "application/json",
                 "API-Version": "2024-01"},
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("errors"):
        raise RuntimeError(f"Monday GraphQL errors: {data['errors']}")
    return data.get("data", {})


def fetch_item_media_url(api_key: str, item_id: str, col_id: str = "text_mkqnx8ze") -> str:
    """Fetch the media plan URL for a single Monday.com item by column ID."""
    query = """
    query ($item_id: ID!, $col_id: String!) {
      items(ids: [$item_id]) {
        column_values(ids: [$col_id]) {
          text
        }
      }
    }
    """
    headers = {"Authorization": api_key, "Content-Type": "application/json"}
    resp = requests.post(
        MONDAY_API_URL,
        json={"query": query, "variables": {"item_id": str(item_id), "col_id": col_id}},
        headers=headers,
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    items = (data.get("data") or {}).get("items") or []
    if not items:
        return ""
    col_vals = items[0].get("column_values") or []
    return (col_vals[0].get("text") or "").strip() if col_vals else ""


def fetch_board_items(api_key: str, board_id: int, limit: int = 500) -> List[Dict]:
    """Fetch all items from a board using cursor pagination."""
    first_q = """
    query ($board_id: ID!, $limit: Int!) {
      boards(ids: [$board_id]) {
        items_page(limit: $limit) {
          cursor
          items {
            id name created_at updated_at
            group { title }
            column_values { id text value type }
          }
        }
      }
    }
    """
    next_q = """
    query ($cursor: String!, $limit: Int!) {
      next_items_page(limit: $limit, cursor: $cursor) {
        cursor
        items {
          id name created_at updated_at
          group { title }
          column_values { id text value type }
        }
      }
    }
    """
    items: List[Dict] = []
    data = _monday_post(api_key, first_q, {"board_id": board_id, "limit": limit})
    page = data["boards"][0]["items_page"]
    items.extend(page["items"])
    cursor = page.get("cursor")
    while cursor:
        data = _monday_post(api_key, next_q, {"cursor": cursor, "limit": limit})
        page = data["next_items_page"]
        items.extend(page["items"])
        cursor = page.get("cursor")
    return items


def _format_col_value(cv: Dict) -> str:
    """Extract readable text from a Monday column_values entry."""
    text = (cv.get("text") or "").strip()
    if text:
        return text
    raw = cv.get("value")
    if not raw:
        return ""
    try:
        data = json.loads(raw) if isinstance(raw, str) else raw
        if not isinstance(data, dict):
            return str(raw).strip()
        cv_type = (cv.get("type") or "").lower()
        # People column
        if "personsAndTeams" in data:
            names = [p.get("name", "") for p in data.get("personsAndTeams", []) if p.get("name")]
            return ", ".join(names)
        # Timeline / date range
        start_keys = ["from", "start", "start_date"]
        end_keys   = ["to", "end", "end_date"]
        if any(k in data for k in start_keys + end_keys):
            parts = []
            for k in start_keys:
                if data.get(k):
                    parts.append(str(data[k]))
                    break
            for k in end_keys:
                if data.get(k):
                    parts.append(str(data[k]))
                    break
            return " – ".join(parts)
        if "date" in data and data["date"]:
            return str(data["date"]).strip()
        # Dropdown / multi-select
        if "labels" in data and isinstance(data["labels"], list):
            return ", ".join(str(x) for x in data["labels"] if x)
        if "label" in data and data["label"]:
            return str(data["label"]).strip()
        return str(data).strip()
    except Exception:
        return str(raw).strip()


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def load_config(config_path: str) -> List[Dict]:
    """Load monday_config.json and return list of board dicts."""
    with open(config_path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    boards = []
    for b in raw.get("boards", []):
        col_map = b.get("column_id_map", {})

        def _col(*keys: str) -> str:
            """Return the first non-empty column ID found for any of the given keys."""
            for k in keys:
                v = col_map.get(k)
                if v and str(v).strip():
                    return str(v).strip()
            return ""

        # Build product column IDs from all product-related keys present in config
        product_col_ids: List[str] = list(filter(None, [
            _col("products_to_pitch", "product_to_propose"),  # primary product column
            _col("product_proposed"),                          # secondary product column
        ]))

        boards.append({
            "region":          b["region"],
            "board_id":        int(b["board_id"]),
            "col_brand":       _col("brand_name"),
            "col_vertical":    _col("vertical"),
            "col_country":     _col("country"),
            "col_run_dates":   _col("run_dates"),
            "col_rfp":         _col("rfp_summary"),
            "col_targeting":   _col("targeting"),
            "col_trigger":     _col("trigger_list"),
            "col_other":       _col("any_other_details"),
            "col_media_plan":  _col("media_plan") or "text_mkqnx8ze",
            "product_col_ids": product_col_ids,
            "platform_col_id": _col("platform_to_pitch"),  # APAC-specific
        })
    return boards


# ---------------------------------------------------------------------------
# Product filtering
# ---------------------------------------------------------------------------

def _mentions_kw(v: str) -> bool:
    s = (v or "").strip().lower()
    return bool(s) and any(kw in s for kw in PRODUCT_MATCH_KEYWORDS)


def should_include(col_values: Dict[str, str], board: Dict) -> bool:
    """
    Return True if the campaign qualifies for inventory check.
    APAC special rule: Products-to-pitch must mention Mirror AND Platform-to-pitch
    must mention YouTube. For all other boards: any product column mentioning
    Mirror/YouTube qualifies; all-blank product columns also qualify.
    """
    product_ids = board.get("product_col_ids", [])
    if not product_ids:
        return True

    product_vals = [str(col_values.get(cid, "") or "").strip() for cid in product_ids]
    any_nonblank = any(v for v in product_vals)
    any_match    = any(_mentions_kw(v) for v in product_vals)

    region = (board.get("region") or "").upper()
    if "APAC" in region:
        platform_id  = board.get("platform_col_id", "")
        platform_val = str(col_values.get(platform_id, "") or "").lower()
        has_youtube  = "youtube" in platform_val
        has_mirror   = any("mirror" in v.lower() for v in product_vals)
        # Qualify if Mirrors 2.0 + YouTube platform, OR all-blank products
        if any_nonblank:
            return has_mirror and has_youtube
        return True  # all blank → include

    # Non-APAC: include if any match or all blank
    if any_nonblank and not any_match:
        return False
    return True


# ---------------------------------------------------------------------------
# Google Sheets reading
# ---------------------------------------------------------------------------

def extract_sheet_id(url: str) -> Optional[str]:
    m = re.search(r"/(?:spreadsheets|file)/d/([a-zA-Z0-9_-]+)", url or "")
    return m.group(1) if m else None


def _is_binary_excel(content_type: str, content: bytes) -> bool:
    if "text/html" in content_type:
        return False
    return len(content) > 4 and content[:2] == b"PK"


def _get_service_account_json_raw() -> str:
    """JSON body for GOOGLE_SERVICE_ACCOUNT_JSON (env or secrets.toml)."""
    v = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    if v.strip():
        return v
    sec = _load_secrets()
    if isinstance(sec, dict):
        raw = sec.get("GOOGLE_SERVICE_ACCOUNT_JSON")
        if isinstance(raw, str) and raw.strip():
            return raw
    return ""


def _optional_google_credentials() -> Any:
    """
    Load service account credentials if configured, else None.
    Configure either:
      - GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
      - GOOGLE_SERVICE_ACCOUNT_JSON='{...}' (full JSON as string in env or secrets.toml)
    Requires: pip install google-auth
    """
    try:
        from google.auth.transport.requests import Request
        from google.oauth2 import service_account
    except ImportError:
        path = _get_env("GOOGLE_APPLICATION_CREDENTIALS")
        if path or _get_service_account_json_raw():
            raise RuntimeError(
                "google-auth is required for service account access. Run: pip install google-auth"
            )
        return None

    scopes = (
        "https://www.googleapis.com/auth/drive.readonly",
        "https://www.googleapis.com/auth/spreadsheets.readonly",
    )
    json_raw = _get_service_account_json_raw()
    path = _get_env("GOOGLE_APPLICATION_CREDENTIALS").strip()
    if not path:
        sec = _load_secrets()
        if isinstance(sec, dict):
            p = sec.get("GOOGLE_APPLICATION_CREDENTIALS")
            if isinstance(p, str) and p.strip():
                path = p.strip()

    path_exp = os.path.expanduser(path) if path else ""

    if json_raw:
        try:
            info = json.loads(json_raw)
        except json.JSONDecodeError as e:
            raise ValueError(f"GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON: {e}") from e
        creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    elif path_exp and os.path.isfile(path_exp):
        creds = service_account.Credentials.from_service_account_file(path_exp, scopes=scopes)
    else:
        return None

    creds.refresh(Request())  # type: ignore[no-untyped-call]
    return creds


def _download_sheet_with_service_account(sheet_id: str, creds: Any) -> Optional[bytes]:
    """Download spreadsheet as .xlsx bytes using OAuth Bearer (service account)."""
    token = creds.token
    headers = {"Authorization": f"Bearer {token}"}

    # 1) Google Sheets host export (works for native Sheets)
    r = requests.get(
        f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx",
        headers=headers,
        timeout=90,
    )
    ct = r.headers.get("content-type", "")
    if r.status_code == 200 and _is_binary_excel(ct, r.content):
        return r.content

    # 2) Drive API export → xlsx (Google Sheets file in Drive)
    r2 = requests.get(
        f"https://www.googleapis.com/drive/v3/files/{sheet_id}/export",
        params={
            "mimeType": (
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            ),
        },
        headers=headers,
        timeout=90,
    )
    ct2 = r2.headers.get("content-type", "")
    if r2.status_code == 200 and _is_binary_excel(ct2, r2.content):
        return r2.content

    # 3) Binary .xlsx uploaded to Drive (not a native Google Sheet)
    r3 = requests.get(
        f"https://www.googleapis.com/drive/v3/files/{sheet_id}",
        params={"alt": "media"},
        headers=headers,
        timeout=90,
    )
    ct3 = r3.headers.get("content-type", "")
    if r3.status_code == 200 and _is_binary_excel(ct3, r3.content):
        return r3.content

    # Helpful error text from API
    detail = ""
    for resp in (r, r2, r3):
        if resp.content and len(resp.content) < 2000 and b"{" in resp.content[:100]:
            try:
                detail = resp.content.decode("utf-8", errors="replace")[:500]
            except Exception:
                pass
            break
    raise PermissionError(
        f"Service account could not download file {sheet_id}. "
        f"Share the file with the service account email (from the JSON: client_email), "
        f"or enable domain-wide access. "
        f"HTTP: export={r.status_code}, drive_export={r2.status_code}, media={r3.status_code}. "
        f"{detail}"
    )


def _drive_download(session: Any, file_id: str) -> Optional[bytes]:
    url  = f"https://drive.google.com/uc?export=download&id={file_id}"
    resp = session.get(url, timeout=60)
    ct   = resp.headers.get("content-type", "")
    if resp.status_code == 200 and _is_binary_excel(ct, resp.content):
        return resp.content
    if resp.status_code == 200 and "text/html" in ct:
        m = (re.search(r'confirm=([0-9A-Za-z_-]+)&', resp.text) or
             re.search(r'confirm=([0-9A-Za-z_-]+)"', resp.text) or
             re.search(r'"downloadUrl":"([^"]+)"', resp.text))
        if m:
            token = m.group(1)
            url2  = token if token.startswith("http") else \
                    f"https://drive.google.com/uc?export=download&id={file_id}&confirm={token}"
            resp2 = session.get(url2, timeout=90)
            ct2   = resp2.headers.get("content-type", "")
            if resp2.status_code == 200 and _is_binary_excel(ct2, resp2.content):
                return resp2.content
    return None


def read_public_sheet(url: str) -> Any:
    """
    Download a Google Sheets / Drive-hosted Excel file.

    If GOOGLE_APPLICATION_CREDENTIALS or GOOGLE_SERVICE_ACCOUNT_JSON is set,
    uses the service account first (for org-restricted or private files shared to SA).

    Otherwise uses unauthenticated download (Anyone-with-the-link).

    Strategy (public): Sheets export URL, then Drive uc?export download.
    Strategy (service account): Bearer export, Drive export, Drive alt=media.
    """
    sheet_id = extract_sheet_id(url)
    if not sheet_id:
        raise ValueError(f"Not a valid Google Sheets / Drive URL: {url}")

    # --- Service account path (optional) ---
    creds = _optional_google_credentials()
    if creds is not None:
        content = _download_sheet_with_service_account(sheet_id, creds)
        return pd.ExcelFile(io.BytesIO(content))

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})

    last_status = None
    last_ct     = ""

    # Strategy 1: native Sheets export (public)
    try:
        resp = session.get(
            f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx",
            timeout=30,
        )
        last_status = resp.status_code
        last_ct     = resp.headers.get("content-type", "")
        if last_status == 200 and _is_binary_excel(last_ct, resp.content):
            return pd.ExcelFile(io.BytesIO(resp.content))
    except Exception:
        pass

    # Strategy 2: Drive direct download (for .xlsx files stored in Drive)
    content = _drive_download(session, sheet_id)
    if content:
        return pd.ExcelFile(io.BytesIO(content))

    # Build a specific reason for the failure
    if last_status == 401:
        reason = (
            "Sharing is 'Anyone in your organisation' — change to "
            "'Anyone with the link → Viewer' (currently requires Google login)"
        )
    elif last_status == 403:
        reason = (
            "Permission denied (HTTP 403) — file may be restricted. "
            "Share → 'Anyone with the link' → Viewer"
        )
    elif last_status and last_status != 200:
        reason = f"HTTP {last_status} returned by Google — sheet may not be public"
    elif "text/html" in last_ct:
        reason = (
            "Google returned a login page — sharing must be "
            "'Anyone with the link → Viewer', not 'Anyone in organisation'"
        )
    else:
        reason = "Could not download sheet — ensure it is shared as 'Anyone with the link → Viewer'"

    raise PermissionError(reason)


def find_context_tab(xls: Any) -> Optional[str]:
    for name in xls.sheet_names:
        if "context" in name.lower():
            return name
    return None


# ---------------------------------------------------------------------------
# Multilingual context row extraction
# ---------------------------------------------------------------------------

def _col_type(header: str) -> Optional[str]:
    h = header.lower().strip()
    if any(sk in h for sk in SUBTACTIC_KEYWORDS):
        return "subtactic"
    if any(tk in h for tk in TACTIC_KEYWORDS):
        return "tactic"
    if any(sk in h for sk in SIGNAL_KEYWORDS):
        return "signal"
    return None


def _detect_lang(header: str) -> str:
    h = header.lower().strip()
    for kw, lang in _TACTIC_LANG.items():
        if kw in h:
            return lang
    for kw, lang in _SIGNAL_LANG.items():
        if kw in h:
            return lang
    return "English"


def read_context_rows(xls: Any, tab_name: str) -> Tuple[List[Dict], str]:
    """
    Parse the context tab.  Returns (rows, local_language).
    Each row: {tactic_en, subtactic_en, signal_en, tactic_local, subtactic_local, signal_local}
    """
    df = xls.parse(tab_name, header=None, dtype=str).fillna("")
    if df.empty:
        return [], ""

    # Find header row (first row containing tactic/signal keyword)
    header_row_idx = None
    for i, row in df.iterrows():
        vals = [str(v).lower() for v in row]
        if any(any(kw in v for kw in TACTIC_KEYWORDS + SIGNAL_KEYWORDS) for v in vals):
            header_row_idx = i
            break
    if header_row_idx is None:
        return [], ""

    headers = [str(v).strip() for v in df.iloc[header_row_idx]]
    col_groups: Dict[str, Dict[str, int]] = {}  # lang -> {tactic/subtactic/signal: col_idx}

    for idx, h in enumerate(headers):
        ct = _col_type(h)
        if not ct:
            continue
        lang = _detect_lang(h)
        col_groups.setdefault(lang, {})[ct] = idx

    if not col_groups:
        return [], ""

    # Identify English vs local language
    en_group    = col_groups.get("English", {})
    local_langs = {l: g for l, g in col_groups.items() if l != "English"}
    local_lang  = next(iter(local_langs), "")
    local_group = local_langs.get(local_lang, {})

    rows: List[Dict] = []
    data_rows = df.iloc[header_row_idx + 1:]

    for _, row_vals in data_rows.iterrows():
        def _get(group: Dict[str, int], col: str) -> str:
            idx = group.get(col)
            if idx is None:
                return ""
            val = str(row_vals.iloc[idx]).strip() if idx < len(row_vals) else ""
            return "" if val.lower() in ("nan", "none", "") else val

        tactic_en = _get(en_group, "tactic")
        signal_en = _get(en_group, "signal")
        if not tactic_en and not signal_en:
            continue
        # Skip if this is a header word itself
        if any(kw in tactic_en.lower() for kw in TACTIC_KEYWORDS):
            continue

        rows.append({
            "tactic_en":    tactic_en,
            "subtactic_en": _get(en_group, "subtactic"),
            "signal_en":    signal_en,
            "tactic_local":    _get(local_group, "tactic"),
            "subtactic_local": _get(local_group, "subtactic"),
            "signal_local":    _get(local_group, "signal"),
        })

    return rows, local_lang


# ---------------------------------------------------------------------------
# Language & category derivation (OpenAI)
# ---------------------------------------------------------------------------

geo_language_map: Dict[str, List[str]] = {
    "Japan": ["Japanese"], "Korea": ["Korean"], "South Korea": ["Korean"],
    "China": ["Chinese (Simplified)", "Chinese (Traditional)"],
    "Taiwan": ["Chinese (Traditional)"], "Hong Kong": ["Chinese (Traditional)", "English"],
    "Thailand": ["Thai"], "Vietnam": ["Vietnamese"], "Indonesia": ["Indonesian"],
    "Malaysia": ["Malay", "English"], "Philippines": ["Filipino", "English"],
    "India": ["English", "Hindi"], "Australia": ["English"], "New Zealand": ["English"],
    "Singapore": ["English"], "UK": ["English"], "US": ["English"],
    "Canada": ["English", "French"], "France": ["French"], "Germany": ["German"],
    "Brazil": ["Portuguese"],
}

_india_lang_map = {
    "hindi": "Hindi", "tamil": "Tamil", "telugu": "Telugu", "kannada": "Kannada",
    "malayalam": "Malayalam", "bengali": "Bengali", "marathi": "Marathi",
    "gujarati": "Gujarati", "punjabi": "Punjabi",
}

language_code_map: Dict[str, List[str]] = {
    "English": ["en"], "Japanese": ["ja"], "Korean": ["ko"],
    "Chinese (Simplified)": ["zh-hans", "zh"], "Chinese (Traditional)": ["zh-hant", "zh"],
    "Thai": ["th"], "Vietnamese": ["vi"], "Indonesian": ["id"],
    "Malay": ["ms"], "Filipino": ["fil", "tl"], "Hindi": ["hi"],
    "Tamil": ["ta"], "Telugu": ["te"], "French": ["fr"],
    "German": ["de"], "Portuguese": ["pt"], "Spanish": ["es"],
}


def _norm_geo(geo: Any) -> str:
    g = str(geo or "").strip().lower()
    if "japan" in g: return "Japan"
    if "korea" in g: return "South Korea"
    if "china" in g: return "China"
    if "taiwan" in g: return "Taiwan"
    if "hong kong" in g: return "Hong Kong"
    if "thai" in g: return "Thailand"
    if "viet" in g: return "Vietnam"
    if "indonesia" in g: return "Indonesia"
    if "malay" in g: return "Malaysia"
    if "philip" in g: return "Philippines"
    if "india" in g: return "India"
    if "australia" in g: return "Australia"
    if "singapore" in g: return "Singapore"
    if "united kingdom" in g or g == "uk": return "UK"
    if "france" in g: return "France"
    if "germany" in g: return "Germany"
    if "brazil" in g: return "Brazil"
    if "canada" in g: return "Canada"
    return str(geo or "").strip()


def derive_languages(geo: Any, brief_text: Any) -> List[str]:
    norm = _norm_geo(geo)
    base = list(geo_language_map.get(norm, ["English"]))
    if norm == "India":
        for kw, lang in _india_lang_map.items():
            if kw in str(brief_text or "").lower() and lang not in base:
                base.append(lang)
    return base


def language_list_to_codes(langs: List[str]) -> List[str]:
    codes: List[str] = []
    for lang in langs:
        for c in language_code_map.get(str(lang).strip(), []):
            if c not in codes:
                codes.append(c)
    return codes


brand_category_map = {"airbnb": ["Travel"]}
vertical_category_map = {
    "foodbeverage": ["FoodBeverage"], "food&beverage": ["FoodBeverage"],
    "beverages": ["FoodBeverage"], "travel": ["Travel"], "sports": ["Sports"],
    "automotive": ["Automobile"], "automobiles": ["Automobile"],
    "finance": ["BusinessFinance"], "businessfinance": ["BusinessFinance"],
    "shopping": ["Shopping"],
}


def derive_categories_with_openai(
    client: Any, brief: str, geo: str, language: str,
    brand: Any, vertical: Any, category_list: List[str],
    context_tactics: str = "",
) -> str:
    brand_cats    = brand_category_map.get(str(brand or "").strip().lower(), [])
    v_key = "".join(c for c in str(vertical or "").lower() if c.isalnum() or c == "&")
    vertical_cats = vertical_category_map.get(v_key, [])
    implied       = list(dict.fromkeys(brand_cats + vertical_cats))

    system = """
You are an expert in contextual targeting for YouTube campaigns.
Choose ALL relevant categories from the provided list. Include Brand-Implied
and Vertical-Implied categories when relevant.

Think in two layers:
- Core categories matching brand, product, vertical.
- Supporting categories reflecting how/where the product is used.

Rules:
1. One or more categories from the list (ordered most to least relevant).
2. Do NOT invent categories outside the list.
3. Return comma-separated category names.
4. If no match, return exactly 'NO_MATCH'.
""".strip()

    context_section = f"\nMedia Plan Context (Tactics/Signals):\n{context_tactics}" if context_tactics else ""

    user = f"""
Brand: {brand}
Vertical: {vertical}
Campaign Brief: {brief}{context_section}
Target Country: {geo}
Target Language: {language}
Brand-Implied Categories: {brand_cats}
Vertical-Implied Categories: {vertical_cats}
Available Categories: {category_list}
""".strip()

    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "system", "content": system},
                  {"role": "user", "content": user}],
        temperature=0,
    )
    raw = resp.choices[0].message.content.strip()

    # Filter to only valid categories
    available = {str(c).strip() for c in category_list}
    model_cats = [x.strip() for x in raw.split(",") if x.strip() in available]
    all_cats = list(dict.fromkeys(model_cats + [c for c in implied if c in available]))
    return ", ".join(all_cats) if all_cats else "NO_MATCH"


# ---------------------------------------------------------------------------
# Inventory check
# ---------------------------------------------------------------------------

def _any_lang_match(val: Any, codes: List[str]) -> bool:
    s = str(val or "").lower().strip()
    return any(c in s for c in codes)


def load_inventory(path: str) -> Tuple[pd.DataFrame, Dict[str, str], List[str]]:
    df = pd.read_csv(path) if path.lower().endswith(".csv") else pd.read_excel(path)
    df.columns = df.columns.str.strip()
    cols = {str(c).strip().lower(): str(c).strip() for c in df.columns}

    def _find(*candidates: str) -> str:
        for cand in candidates:
            for k, v in cols.items():
                if cand in k:
                    return v
        return ""

    inv_cols = {
        "category":             _find("category"),
        "channelid":            _find("channelid", "channel_id", "channel id"),
        "model_video_language": _find("model_video_language", "video language"),
        "resource_language":    _find("resource_language", "resource language"),
        "model_channel_language": _find("model_channel_language", "channel language"),
        "description_language": _find("description_language", "description language"),
    }
    cat_col = inv_cols["category"]
    categories = df[cat_col].dropna().astype(str).unique().tolist() if cat_col else []
    return df, inv_cols, categories


def check_inventory(
    inventory_df: pd.DataFrame, inv_cols: Dict[str, str],
    categories: Any, lang_codes: List[str],
) -> Dict[str, Any]:
    if not categories or not lang_codes:
        return {"p1": 0, "p2": 0, "p3": 0, "total": 0, "status": "Nil"}
    cats = [c.strip() for c in (categories.split(",") if isinstance(categories, str) else categories)
            if c.strip() and c.strip() not in ("NO_MATCH",) and not str(c).startswith("ERROR")]
    if not cats:
        return {"p1": 0, "p2": 0, "p3": 0, "total": 0, "status": "Nil"}

    df = inventory_df
    base = df[inv_cols["category"]].fillna("").isin(cats)
    ch   = df[inv_cols["channelid"]].fillna("") if inv_cols.get("channelid") else df.index.to_series()
    mv   = df[inv_cols["model_video_language"]].fillna("").apply(lambda v: _any_lang_match(v, lang_codes))
    rl   = df[inv_cols["resource_language"]].fillna("") if inv_cols.get("resource_language") else pd.Series([""] * len(df))
    mcl  = df[inv_cols["model_channel_language"]].fillna("").apply(lambda v: _any_lang_match(v, lang_codes)) if inv_cols.get("model_channel_language") else pd.Series([False] * len(df))
    dl   = df[inv_cols["description_language"]].fillna("").apply(lambda v: _any_lang_match(v, lang_codes)) if inv_cols.get("description_language") else pd.Series([False] * len(df))
    rl_match = rl.apply(lambda v: _any_lang_match(v, lang_codes))
    rl_blank = rl.astype(str).str.strip().eq("")

    p1_ch = set(ch[base & mv & rl_match & mcl].tolist())
    p2_ch = set(ch[base & mv & rl_match].tolist()) - p1_ch
    p3_ch = set(ch[base & rl_blank & mv & mcl & dl].tolist()) - p1_ch - p2_ch

    def _cnt(s: set) -> int:
        return len([x for x in s if str(x).strip()])

    p1, p2, p3 = _cnt(p1_ch), _cnt(p2_ch), _cnt(p3_ch)
    total = p1 + p2 + p3
    status = "Nil" if total == 0 else "Low" if total <= 100 else "Medium" if total <= 500 else "Okay"
    return {"p1": p1, "p2": p2, "p3": p3, "total": total, "status": status}


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _insert_campaign(conn, run_id: str, item_id: str, board_id: int,
                     region: str, data: Dict) -> None:
    now = utc_now_iso()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO campaigns (
              run_id, monday_item_id, monday_board_id, monday_url, region,
              campaign_name, brand_name, vertical, country, run_dates,
              rfp_summary, targeting, trigger_list, any_other_details,
              products_to_pitch, monday_submitted_at, media_plan_url, inserted_at_utc
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (monday_item_id) DO NOTHING
        """, (
            run_id, item_id, str(board_id),
            data["monday_url"], region,
            data.get("campaign_name"), data.get("brand_name"),
            data.get("vertical"), data.get("country"), data.get("run_dates"),
            data.get("rfp_summary"), data.get("targeting"),
            data.get("trigger_list"), data.get("any_other_details"),
            data.get("products_to_pitch"), data.get("monday_submitted_at"),
            data.get("media_plan_url"), now,
        ))
    conn.commit()


def _update_context_status(conn, item_id: str, status: str) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE campaigns
            SET context_status=%s, updated_at_utc=%s
            WHERE monday_item_id=%s
        """, (status, utc_now_iso(), item_id))
    conn.commit()


def _update_campaign_analysis(conn, item_id: str, language: str, category: str,
                               recommendation_basis: str = "", error: str = "") -> None:
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE campaigns
            SET derived_language=%s, recommended_category=%s,
                recommendation_basis=%s, error_log=%s, updated_at_utc=%s
            WHERE monday_item_id=%s
        """, (language, category, recommendation_basis or None, error or None, utc_now_iso(), item_id))
    conn.commit()


def _update_campaign_inventory(conn, item_id: str, inv: Dict) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE campaigns
            SET inventory_status=%s, available_inventory_count=%s,
                p1_channel_count=%s, p2_channel_count=%s, p3_channel_count=%s,
                updated_at_utc=%s
            WHERE monday_item_id=%s
        """, (inv["status"], inv["total"], inv["p1"], inv["p2"], inv["p3"],
              utc_now_iso(), item_id))
    conn.commit()


def _upsert_alert(conn, campaign: Dict) -> None:
    with conn.cursor() as cur:
        # Only insert if no open alert for this campaign already exists
        cur.execute(
            "SELECT 1 FROM alerts WHERE monday_item_id=%s AND resolved_at_utc IS NULL LIMIT 1",
            (campaign["monday_item_id"],),
        )
        if cur.fetchone():
            return
        cur.execute("""
            INSERT INTO alerts (
              monday_item_id, monday_url, region, campaign_name, brand_name,
              country, derived_language, products_to_pitch, monday_run_dates,
              monday_submitted_at_utc, recommended_category,
              inventory_status, p1_channel_count, p2_channel_count,
              p3_channel_count, available_inventory_count, error_log,
              date_flagged_utc
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            campaign["monday_item_id"], campaign.get("monday_url"),
            campaign.get("region"), campaign.get("campaign_name"),
            campaign.get("brand_name"), campaign.get("country"),
            campaign.get("derived_language"), campaign.get("products_to_pitch"),
            campaign.get("run_dates"), campaign.get("monday_submitted_at"),
            campaign.get("recommended_category"),
            campaign.get("inventory_status"),
            campaign.get("p1_channel_count"), campaign.get("p2_channel_count"),
            campaign.get("p3_channel_count"), campaign.get("available_inventory_count"),
            campaign.get("error_log"), utc_now_iso(),
        ))
    conn.commit()


def _upsert_blocked(conn, run_id: str, item_id: str, board_id: int,
                    meta: Dict, error: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM access_blocked WHERE monday_item_id=%s AND resolved_at_utc IS NULL LIMIT 1",
            (str(item_id),),
        )
        if cur.fetchone():
            cur.execute("""
                UPDATE access_blocked
                SET error_message=%s, date_flagged_utc=%s, run_id=%s
                WHERE monday_item_id=%s AND resolved_at_utc IS NULL
            """, (error, utc_now_iso(), run_id, str(item_id)))
        else:
            cur.execute("""
                INSERT INTO access_blocked (
                  run_id, monday_item_id, monday_board_id, monday_url, region,
                  campaign_name, brand, country, media_plan_url,
                  error_message, date_flagged_utc
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                run_id, str(item_id), str(board_id),
                meta.get("monday_url"), meta.get("region"),
                meta.get("campaign_name"), meta.get("brand_name"),
                meta.get("country"), meta.get("media_plan_url"),
                error, utc_now_iso(),
            ))
    conn.commit()


def _insert_context_rows(conn, run_id: str, item_id: str, board_id: int,
                          meta: Dict, rows: List[Dict], local_lang: str) -> None:
    now = utc_now_iso()
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, """
            INSERT INTO context_rows (
              run_id, monday_item_id, monday_board_id, monday_url,
              region, campaign_name, brand, country, vertical, brief,
              local_language,
              tactic_en, subtactic_en, signal_en,
              tactic_local, subtactic_local, signal_local, inserted_at_utc
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, [
            (run_id, item_id, str(board_id), meta.get("monday_url"),
             meta.get("region"), meta.get("campaign_name"), meta.get("brand_name"),
             meta.get("country"), meta.get("vertical"), meta.get("brief"),
             local_lang,
             r.get("tactic_en"), r.get("subtactic_en"), r.get("signal_en"),
             r.get("tactic_local"), r.get("subtactic_local"), r.get("signal_local"),
             now)
            for r in rows
        ])
    conn.commit()


def _already_complete(conn, item_id: str) -> bool:
    """Return True if the campaign has already been fully processed (inventory done)."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM campaigns WHERE monday_item_id=%s AND inventory_status IS NOT NULL LIMIT 1",
            (str(item_id),),
        )
        return cur.fetchone() is not None


def _get_campaigns_needing_analysis(conn) -> List[Dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT monday_item_id, monday_url, region, campaign_name, brand_name,
                   vertical, country, run_dates, rfp_summary, targeting,
                   any_other_details, products_to_pitch, monday_submitted_at
            FROM campaigns
            WHERE derived_language IS NULL
        """)
        return [dict(r) for r in cur.fetchall()]


def _get_campaigns_needing_inventory(conn) -> List[Dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT monday_item_id, monday_url, region, campaign_name, brand_name,
                   vertical, country, run_dates, rfp_summary, targeting,
                   any_other_details, products_to_pitch, monday_submitted_at,
                   derived_language, recommended_category, error_log
            FROM campaigns
            WHERE derived_language IS NOT NULL AND inventory_status IS NULL
        """)
        return [dict(r) for r in cur.fetchall()]


def _get_context_tactics(conn, item_id: str) -> str:
    """Return a short text summary of context rows for the OpenAI prompt."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT tactic_en, signal_en FROM context_rows WHERE monday_item_id=%s LIMIT 20",
            (item_id,),
        )
        rows = cur.fetchall()
    if not rows:
        return ""
    lines = [f"- {r[0]} / {r[1]}" for r in rows if r[0] or r[1]]
    return "\n".join(lines)


def _log_run_start(conn, run_id: str, started_at: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO pipeline_runs (run_id, started_at_utc, status) VALUES (%s,%s,'running') "
            "ON CONFLICT (run_id) DO NOTHING",
            (run_id, started_at),
        )
    conn.commit()


def _log_run_finish(conn, run_id: str, status: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE pipeline_runs SET finished_at_utc=%s, status=%s WHERE run_id=%s",
            (utc_now_iso(), status, run_id),
        )
    conn.commit()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Unified Presales Pipeline")
    parser.add_argument("config",    help="Path to monday_config.json")
    parser.add_argument("inventory", help="Path to inventory file (xlsx or csv)")
    parser.add_argument("--since",    default=None,
                        help=f"Process campaigns created on/after YYYY-MM-DD "
                             f"(default: last successful run or {FIRST_RUN_SINCE})")
    parser.add_argument("--max-step", type=int, default=4, choices=[1, 2, 3, 4],
                        help="Stop after this step (1=Monday fetch, 2=+Sheets, 3=+OpenAI, 4=+Inventory). Default: 4 (all steps)")
    args = parser.parse_args()

    monday_key = _get_env("MONDAY_API_KEY")
    openai_key = _get_env("OPENAI_API_KEY")
    if not monday_key:
        raise SystemExit("MONDAY_API_KEY is not set.")
    if not openai_key:
        raise SystemExit("OPENAI_API_KEY is not set.")
    if OpenAI is None:
        raise SystemExit("openai package not installed. Run: pip3 install openai")

    conn = get_db()
    init_schema(conn)

    # Determine since-date
    if args.since:
        since_date = args.since
    else:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT started_at_utc FROM pipeline_runs WHERE status='success' "
                "ORDER BY started_at_utc DESC LIMIT 1"
            )
            row = cur.fetchone()
        since_date = str(row[0])[:10] if row else FIRST_RUN_SINCE

    try:
        since_dt = datetime.strptime(since_date, "%Y-%m-%d")
    except ValueError:
        since_dt = datetime.strptime(FIRST_RUN_SINCE, "%Y-%m-%d")
        since_date = FIRST_RUN_SINCE

    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    _log_run_start(conn, run_id, utc_now_iso())
    print(f"\n{'='*60}")
    print(f"Pipeline run: {run_id}  |  since: {since_date}")
    print(f"{'='*60}")

    boards = load_config(args.config)

    # ────────────────────────────────────────────────────────────────────────
    # STEP 1 — Monday.com → campaigns table
    # ────────────────────────────────────────────────────────────────────────
    print("\n── STEP 1: Fetching campaigns from Monday.com")
    new_campaigns = 0
    for board in boards:
        region   = board["region"]
        board_id = board["board_id"]
        print(f"\n   Board: {region}")

        try:
            items = fetch_board_items(monday_key, board_id)
        except Exception as e:
            print(f"   ERROR: {e}")
            continue

        saved = skipped_date = skipped_done = skipped_product = 0
        for item in items:
            group = (item.get("group") or {}).get("title", "")
            if not group.lower().startswith("done"):
                continue

            # Date filter
            created_str = item.get("created_at") or ""
            try:
                created_dt = datetime.fromisoformat(created_str.replace("Z", "+00:00")).replace(tzinfo=None)
                if created_dt < since_dt:
                    skipped_date += 1
                    continue
            except Exception:
                pass

            item_id = str(item["id"])

            # Already fully processed
            if _already_complete(conn, item_id):
                skipped_done += 1
                continue

            col_values = {cv["id"]: _format_col_value(cv) for cv in item.get("column_values", [])}

            # Product filter
            if not should_include(col_values, board):
                skipped_product += 1
                continue

            monday_url    = MONDAY_ITEM_URL.format(board_id=board_id, item_id=item_id)
            product_parts = [str(col_values.get(cid, "") or "") for cid in board["product_col_ids"]]

            # For APAC, also include Platform to pitch so the stored value reflects
            # the full combination used for filtering (e.g. "Mirrors 2.0 | YouTube")
            if "APAC" in region.upper():
                platform_val = str(col_values.get(board.get("platform_col_id", ""), "") or "").strip()
                if platform_val and platform_val not in product_parts:
                    product_parts.append(platform_val)

            data = {
                "monday_url":        monday_url,
                "campaign_name":     item.get("name", ""),
                "brand_name":        col_values.get(board["col_brand"], ""),
                "vertical":          col_values.get(board["col_vertical"], ""),
                "country":           col_values.get(board["col_country"], ""),
                "run_dates":         col_values.get(board["col_run_dates"], ""),
                "rfp_summary":       col_values.get(board["col_rfp"], ""),
                "targeting":         col_values.get(board["col_targeting"], ""),
                "trigger_list":      col_values.get(board["col_trigger"], ""),
                "any_other_details": col_values.get(board["col_other"], ""),
                "products_to_pitch":   " | ".join(filter(None, product_parts)),
                "monday_submitted_at": created_str,
                "media_plan_url":      col_values.get(board["col_media_plan"], ""),
            }

            _insert_campaign(conn, run_id, item_id, board_id, region, data)
            saved += 1
            new_campaigns += 1

        print(f"   Saved: {saved} | Skipped (date): {skipped_date} | "
              f"Skipped (done): {skipped_done} | Skipped (product): {skipped_product}")

    print(f"\n   ✓ Step 1 complete — {new_campaigns} new campaign(s) saved to Supabase")

    # ────────────────────────────────────────────────────────────────────────
    # STEP 2 — Google Sheets → context_rows + access_blocked
    # ────────────────────────────────────────────────────────────────────────
    print("\n── STEP 2: Reading media plans (Google Sheets)")

    # Fetch campaigns that don't yet have context rows — media_plan_url already stored in DB
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT c.monday_item_id, c.monday_board_id, c.monday_url, c.region,
                   c.campaign_name, c.brand_name, c.country, c.vertical,
                   c.rfp_summary, c.targeting, c.any_other_details, c.media_plan_url
            FROM campaigns c
            WHERE NOT EXISTS (
                SELECT 1 FROM context_rows cr WHERE cr.monday_item_id = c.monday_item_id
            )
            AND c.context_status IS NULL
        """)
        campaigns_needing_context = [dict(r) for r in cur.fetchall()]

    ctx_processed = ctx_blocked = ctx_no_link = 0
    for camp in campaigns_needing_context:
        item_id        = camp["monday_item_id"]
        board_id       = camp.get("monday_board_id", "")
        media_plan_url = camp.get("media_plan_url") or ""

        if not media_plan_url:
            _update_context_status(conn, item_id, "❌ Media plan link not set on Monday.com")
            ctx_no_link += 1
            continue

        meta = {**camp, "brief": " | ".join(filter(None, [
            camp.get("rfp_summary"), camp.get("targeting"), camp.get("any_other_details"),
        ])), "media_plan_url": media_plan_url}

        print(f"   Reading: {camp.get('campaign_name')} ({camp.get('region')})")
        try:
            xls = read_public_sheet(media_plan_url)
        except PermissionError as e:
            reason = str(e)
            print(f"   BLOCKED: {reason}")
            _upsert_blocked(conn, run_id, item_id, int(board_id or 0), meta, reason)
            _update_context_status(conn, item_id, f"❌ Access blocked – fix: Share → Anyone with the link → Viewer")
            ctx_blocked += 1
            continue
        except Exception as e:
            reason = str(e)
            print(f"   FAILED: {reason}")
            _upsert_blocked(conn, run_id, item_id, int(board_id or 0), meta, reason)
            _update_context_status(conn, item_id, f"❌ Could not read media plan – {reason}")
            ctx_blocked += 1
            continue

        tab = find_context_tab(xls)
        if not tab:
            _upsert_blocked(conn, run_id, item_id, int(board_id or 0), meta, "No 'context' tab found")
            _update_context_status(conn, item_id, "❌ No 'Context' tab found in media plan")
            ctx_blocked += 1
            continue

        try:
            ctx_rows, local_lang = read_context_rows(xls, tab)
        except Exception as e:
            reason = f"Context parse error: {e}"
            _upsert_blocked(conn, run_id, item_id, int(board_id or 0), meta, reason)
            _update_context_status(conn, item_id, f"❌ {reason}")
            ctx_blocked += 1
            continue

        if ctx_rows:
            _insert_context_rows(conn, run_id, item_id, int(board_id or 0), meta, ctx_rows, local_lang)
            lang_label = local_lang or "English"
            print(f"   ✓ {len(ctx_rows)} context rows saved | lang: {lang_label}")
            _update_context_status(conn, item_id, f"✅ {len(ctx_rows)} rows saved ({lang_label})")
            ctx_processed += 1
        else:
            _upsert_blocked(conn, run_id, item_id, int(board_id or 0), meta, "Context list not in standard format")
            _update_context_status(conn, item_id, "❌ Context list not in standard format")
            ctx_blocked += 1

    print(f"\n   ✓ Step 2 complete — Processed: {ctx_processed} | "
          f"Blocked: {ctx_blocked} | No link: {ctx_no_link}")

    if args.max_step < 3:
        _log_run_finish(conn, run_id, "success")
        conn.close()
        print(f"\n{'='*60}")
        print(f"✅ Pipeline stopped after Step 2 (--max-step {args.max_step})")
        print(f"   Verify data in Supabase, then re-run without --max-step to complete.")
        print(f"{'='*60}\n")
        return

    # ────────────────────────────────────────────────────────────────────────
    # STEP 3 — OpenAI → update campaigns (language + category)
    # ────────────────────────────────────────────────────────────────────────
    print("\n── STEP 3: OpenAI — deriving language & category")
    client = OpenAI(api_key=openai_key)
    campaigns_to_analyse = _get_campaigns_needing_analysis(conn)
    print(f"   {len(campaigns_to_analyse)} campaign(s) to analyse")

    for camp in campaigns_to_analyse:
        item_id = camp["monday_item_id"]
        name    = camp.get("campaign_name", "")
        geo     = camp.get("country", "")
        brief   = " | ".join(filter(None, [
            camp.get("rfp_summary"), camp.get("targeting"), camp.get("any_other_details"),
        ]))
        print(f"   Analysing: {name} ({camp.get('region')})")

        # Load inventory categories for OpenAI prompt
        inventory_df, inv_cols, category_list = load_inventory(args.inventory)

        lang_list = derive_languages(geo, brief)
        language  = ", ".join(lang_list)

        context_tactics = _get_context_tactics(conn, item_id)
        basis = "Input + Context List" if context_tactics else "Input only"

        try:
            category = derive_categories_with_openai(
                client=client, brief=brief, geo=str(geo), language=language,
                brand=camp.get("brand_name"), vertical=camp.get("vertical"),
                category_list=category_list, context_tactics=context_tactics,
            )
            _update_campaign_analysis(conn, item_id, language, category, basis)
            print(f"   ✓ Language: {language} | Category: {category} | Basis: {basis}")
        except Exception as e:
            _update_campaign_analysis(conn, item_id, language, f"ERROR: {e}", basis, str(e))
            print(f"   ERROR: {e}")

    print(f"\n   ✓ Step 3 complete")

    if args.max_step < 4:
        _log_run_finish(conn, run_id, "success")
        conn.close()
        print(f"\n{'='*60}")
        print(f"✅ Pipeline stopped after Step 3 (--max-step {args.max_step})")
        print(f"   Verify language/category in Supabase, then re-run without --max-step.")
        print(f"{'='*60}\n")
        return

    # ────────────────────────────────────────────────────────────────────────
    # STEP 4 — Inventory check → update campaigns + alerts
    # ────────────────────────────────────────────────────────────────────────
    print("\n── STEP 4: Inventory check")
    inventory_df, inv_cols, _ = load_inventory(args.inventory)
    campaigns_for_inventory = _get_campaigns_needing_inventory(conn)
    print(f"   {len(campaigns_for_inventory)} campaign(s) to check")

    for camp in campaigns_for_inventory:
        item_id  = camp["monday_item_id"]
        name     = camp.get("campaign_name", "")
        cats     = camp.get("recommended_category", "")
        geo      = camp.get("country", "")
        lang     = camp.get("derived_language", "")
        lang_list = [l.strip() for l in lang.split(",") if l.strip()]
        codes    = language_list_to_codes(lang_list) or ["en"]

        print(f"   Checking: {name} | Category: {cats}")

        inv = check_inventory(inventory_df, inv_cols, cats, codes)
        _update_campaign_inventory(conn, item_id, inv)
        print(f"   ✓ Status: {inv['status']} | Total: {inv['total']} "
              f"(P1:{inv['p1']} P2:{inv['p2']} P3:{inv['p3']})")

        # Raise alert for low/nil inventory
        if inv["status"] in ("Low", "Nil"):
            full = {**camp, "inventory_status": inv["status"],
                    "available_inventory_count": inv["total"],
                    "p1_channel_count": inv["p1"], "p2_channel_count": inv["p2"],
                    "p3_channel_count": inv["p3"]}
            _upsert_alert(conn, full)
            print(f"   ⚠ Alert raised: {inv['status']}")

    print(f"\n   ✓ Step 4 complete")

    # ────────────────────────────────────────────────────────────────────────
    # Finalise
    # ────────────────────────────────────────────────────────────────────────
    _log_run_finish(conn, run_id, "success")
    conn.close()
    print(f"\n{'='*60}")
    print(f"✅ Pipeline complete — Run: {run_id}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
