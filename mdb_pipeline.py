"""
MDB Pipeline — fetches "Done" campaigns from Monday.com,
reads media plan Google Sheets, extracts multilingual context rows
(Tactic / Sub-Tactic / Signal), and stores everything in Supabase.

Usage:
  python3 mdb_pipeline.py <monday_config.json> [--since YYYY-MM-DD]

  --since  Only process campaigns updated on or after this date.
           Defaults to 30 days ago.

Secrets (env vars or .streamlit/secrets.toml):
  MONDAY_API_KEY
  DATABASE_URL
"""

import io
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    psycopg2 = None  # type: ignore


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MONDAY_API_URL = "https://api.monday.com/v2"
MONDAY_ITEM_URL = "https://silverpush-global.monday.com/boards/{board_id}/pulses/{item_id}"
PRESALES_DIR = Path(__file__).resolve().parent

# Multilingual keyword lists for column detection
TACTIC_KEYWORDS    = ["tactic", "tactique", "táctica", "tactiek", "rणनीति", "taktik", "戦術"]
SUBTACTIC_KEYWORDS = ["sub-tactic", "subtactic", "sub tactic", "sous-tactic",
                      "sous tactique", "sub-táctica", "sub-taktik", "サブタクティック"]
SIGNAL_KEYWORDS    = ["signal", "señal", "signaal", "संकेत", "シグナル"]

# Language detection: keyword → language name
_TACTIC_LANG: Dict[str, str] = {
    "tactique":  "French",
    "táctica":   "Spanish",
    "tactiek":   "Dutch",
    "taktik":    "German",
    "戦術":       "Japanese",
    "rणनीति":   "Hindi",
    "tactic":    "English",
}
_SIGNAL_LANG: Dict[str, str] = {
    "señal":    "Spanish",
    "signaal":  "Dutch",
    "संकेत":    "Hindi",
    "シグナル":  "Japanese",
    "signal":   "English",
}

PRODUCT_MATCH_KEYWORDS = ["mirror", "mirrors", "youtube"]


# ---------------------------------------------------------------------------
# Secrets / DB
# ---------------------------------------------------------------------------

def _load_secrets() -> Dict[str, str]:
    secrets_path = PRESALES_DIR / ".streamlit" / "secrets.toml"
    if not secrets_path.exists():
        return {}
    try:
        try:
            import tomllib  # Python 3.11+
        except ImportError:
            import tomli as tomllib  # type: ignore
        with open(secrets_path, "rb") as f:
            return tomllib.load(f)
    except Exception:
        return {}


def _get_env(key: str) -> str:
    val = os.getenv(key, "").strip()
    if not val:
        val = _load_secrets().get(key, "").strip()
    return val


def get_db():
    if psycopg2 is None:
        raise RuntimeError("psycopg2 not installed. Run: pip3 install psycopg2-binary")
    url = _get_env("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is not set in environment or .streamlit/secrets.toml")
    conn = psycopg2.connect(url)
    conn.autocommit = False
    return conn


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ---------------------------------------------------------------------------
# Schema (new tables)
# ---------------------------------------------------------------------------

def init_mdb_db(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS mdb_runs (
              run_id          TEXT PRIMARY KEY,
              started_at_utc  TEXT NOT NULL,
              finished_at_utc TEXT,
              status          TEXT NOT NULL,
              processed_count INTEGER DEFAULT 0,
              blocked_count   INTEGER DEFAULT 0,
              skipped_count   INTEGER DEFAULT 0,
              failed_count    INTEGER DEFAULT 0
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
              tactic_en        TEXT,
              subtactic_en     TEXT,
              signal_en        TEXT,
              tactic_local     TEXT,
              subtactic_local  TEXT,
              signal_local     TEXT,
              local_language   TEXT,
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
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_context_rows_campaign
            ON context_rows(region, campaign_name);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_access_blocked_open
            ON access_blocked(resolved_at_utc, region);
        """)
    conn.commit()


# ---------------------------------------------------------------------------
# Monday API
# ---------------------------------------------------------------------------

def _monday_post(api_key: str, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
    resp = requests.post(
        MONDAY_API_URL,
        headers={"Authorization": api_key, "API-Version": "2024-01"},
        json={"query": query, "variables": variables},
        timeout=60,
    )
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("errors"):
        raise RuntimeError(f"Monday GraphQL errors: {payload['errors']}")
    return payload["data"]


def fetch_done_items(api_key: str, board_id: str, col_ids: List[str]) -> List[Dict[str, Any]]:
    """Fetch items with status 'Done' from a Monday board (paginated).
    Monday API requires query_params only on the first page and cursor only on subsequent pages.
    """
    col_ids_gql = ", ".join(f'"{c}"' for c in col_ids)

    first_page_query = f"""
    query($board_id: ID!, $limit: Int!) {{
      boards(ids: [$board_id]) {{
        items_page(limit: $limit, query_params: {{
          rules: [{{ column_id: "color_mkqjerxs",
                     compare_value: [COMPARE_VALUE],
                     operator: any_of }}]
        }}) {{
          cursor
          items {{
            id name updated_at created_at
            column_values(ids: [{col_ids_gql}]) {{
              id text value type
            }}
          }}
        }}
      }}
    }}"""

    next_page_query = f"""
    query($cursor: String!, $limit: Int!) {{
      next_items_page(limit: $limit, cursor: $cursor) {{
        cursor
        items {{
          id name updated_at created_at
          column_values(ids: [{col_ids_gql}]) {{
            id text value type
          }}
        }}
      }}
    }}"""

    def _fetch_by(compare_value: str) -> List[Dict[str, Any]]:
        # First page — use query_params with status filter
        q = first_page_query.replace("COMPARE_VALUE", compare_value)
        data = _monday_post(api_key, q, {"board_id": board_id, "limit": 100})
        page = data["boards"][0]["items_page"]
        items: List[Dict[str, Any]] = list(page["items"])
        cursor = page.get("cursor")

        # Subsequent pages — use cursor only (no query_params)
        while cursor:
            data = _monday_post(api_key, next_page_query,
                                {"cursor": cursor, "limit": 100})
            page = data["next_items_page"]
            items.extend(page["items"])
            cursor = page.get("cursor")

        return items

    # Try by index first (1 = Done), fall back to label "Done"
    items = _fetch_by("1")
    if not items:
        items = _fetch_by('"Done"')
    return items


def _format_col_value(cv: Dict[str, Any]) -> str:
    """Extract a human-readable string from a Monday column_value object."""
    text = (cv.get("text") or "").strip()
    if text:
        return text
    raw = cv.get("value")
    if not raw:
        return ""
    try:
        data = json.loads(raw) if isinstance(raw, str) else raw
    except Exception:
        return str(raw).strip()

    cv_type = (cv.get("type") or "").lower()
    if isinstance(data, dict):
        if "dropdown" in cv_type or "multi_select" in cv_type:
            labels = data.get("labels")
            if isinstance(labels, list) and labels:
                return ", ".join(str(x).strip() for x in labels if x is not None and str(x).strip())
            label = data.get("label")
            if label:
                return str(label).strip()
            sv = data.get("selected_value")
            if sv:
                return str(sv).strip()
        for sk in ["from", "start", "start_date"]:
            if sk in data:
                s = str(data[sk] or "").strip()
                e = str(data.get("to") or data.get("end") or data.get("end_date") or "").strip()
                return f"{s} - {e}" if s and e else s or e
    return str(data).strip()


# ---------------------------------------------------------------------------
# Product filtering
# ---------------------------------------------------------------------------

def _has_keyword(value: str, keywords: List[str]) -> bool:
    v = (value or "").strip().lower()
    return any(kw in v for kw in keywords)


def should_include_campaign(col_values: Dict[str, str], board: Dict[str, Any]) -> bool:
    """
    APAC: include if (products_to_pitch has mirror AND platform_to_pitch has youtube)
          OR product_proposed has youtube/mirror.
    All others: include if ANY product column has mirror or youtube.
    """
    if board["region"] == "APAC":
        p2p  = col_values.get("dropdown_mkt2dahw", "")   # Products to pitch
        plat = col_values.get("dropdown_mkt29d1z", "")   # Platform to pitch
        pp   = col_values.get("dropdown_mkqjq079", "")   # Product proposed

        if _has_keyword(pp, PRODUCT_MATCH_KEYWORDS):
            return True
        if _has_keyword(p2p, ["mirror", "mirrors"]) and _has_keyword(plat, ["youtube"]):
            return True
        return False
    else:
        return any(_has_keyword(col_values.get(cid, ""), PRODUCT_MATCH_KEYWORDS)
                   for cid in board["products"])


# ---------------------------------------------------------------------------
# Google Sheets reading (public / anyone-with-link)
# ---------------------------------------------------------------------------

def extract_sheet_id(url: str) -> Optional[str]:
    # Handle both Google Sheets URLs and Drive file URLs
    m = re.search(r"/(?:spreadsheets|file)/d/([a-zA-Z0-9_-]+)", url or "")
    return m.group(1) if m else None


def _is_binary_excel(content_type: str, content: bytes) -> bool:
    """Return True if the response looks like a real Excel/binary file, not an HTML page."""
    if "text/html" in content_type:
        return False
    # Double-check first bytes: XLSX files start with PK (zip magic 0x504B)
    return len(content) > 4 and content[:2] == b"PK"


def _drive_download(session: "requests.Session", file_id: str) -> Optional[bytes]:
    """
    Download a file from Google Drive for 'Anyone with the link' files.
    Handles both direct downloads and Google's virus-scan confirmation page
    (shown for larger files).
    """
    url = f"https://drive.google.com/uc?export=download&id={file_id}"
    resp = session.get(url, timeout=60)
    ct = resp.headers.get("content-type", "")

    if resp.status_code == 200 and _is_binary_excel(ct, resp.content):
        return resp.content

    # Google shows an HTML confirmation page for larger files — extract the token
    if resp.status_code == 200 and "text/html" in ct:
        # Try the newer `uuid` confirm pattern first, then the classic `confirm=T` pattern
        m = re.search(r'confirm=([0-9A-Za-z_-]+)&', resp.text) or \
            re.search(r'confirm=([0-9A-Za-z_-]+)"', resp.text) or \
            re.search(r'"downloadUrl":"([^"]+)"', resp.text)
        if m:
            token_or_url = m.group(1)
            if token_or_url.startswith("http"):
                confirm_url = token_or_url
            else:
                confirm_url = f"https://drive.google.com/uc?export=download&id={file_id}&confirm={token_or_url}"
            resp2 = session.get(confirm_url, timeout=90)
            ct2 = resp2.headers.get("content-type", "")
            if resp2.status_code == 200 and _is_binary_excel(ct2, resp2.content):
                return resp2.content

    return None


def read_public_sheet(url: str) -> Any:
    """
    Download a Google Sheets / Drive-hosted Excel file for public
    'Anyone with the link can view' files.

    Mirrors the Apps Script logic: tries the native Google Sheets export URL
    first (works for native .gsheet files), then falls back to the Google Drive
    direct-download URL (needed for .xlsx files uploaded to Drive and opened in
    Sheets view — they show the .XLSX badge in the browser).

    Raises PermissionError if neither method succeeds (file is not public).
    Raises ValueError for unrecognisable URLs.
    """
    sheet_id = extract_sheet_id(url)
    if not sheet_id:
        raise ValueError(f"Not a valid Google Sheets / Drive URL: {url}")

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})

    # ── Strategy 1: Google Sheets export URL (native .gsheet files) ──────────
    export_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx"
    try:
        resp = session.get(export_url, timeout=30)
        ct = resp.headers.get("content-type", "")
        if resp.status_code == 200 and _is_binary_excel(ct, resp.content):
            return pd.ExcelFile(io.BytesIO(resp.content))
    except Exception:
        pass

    # ── Strategy 2: Google Drive download (Excel .xlsx files in Drive) ───────
    # This is the Python equivalent of the Apps Script's Drive.Files.copy()
    # conversion: instead of converting, we download the raw Excel bytes directly.
    content = _drive_download(session, sheet_id)
    if content:
        return pd.ExcelFile(io.BytesIO(content))

    # Both methods returned HTML — file requires login or is not shared publicly
    raise PermissionError(
        f"Cannot access media plan (not public or login required): {url}\n"
        f"Fix: open the Google Sheet/Drive file → Share → 'Anyone with the link' → Viewer"
    )


def find_context_tab(xls: Any) -> Optional[str]:
    for name in xls.sheet_names:
        if "context" in name.lower():
            return name
    return None


# ---------------------------------------------------------------------------
# Multilingual column detection
# ---------------------------------------------------------------------------

def _detect_col_language(header: str) -> str:
    h = header.lower().strip()
    for kw, lang in _TACTIC_LANG.items():
        if kw in h:
            return lang
    for kw, lang in _SIGNAL_LANG.items():
        if kw in h:
            return lang
    return "Unknown"


def _col_type(header: str) -> Optional[str]:
    h = header.lower().strip()
    if any(sk in h for sk in SUBTACTIC_KEYWORDS):
        return "subtactic"
    if any(tk in h for tk in TACTIC_KEYWORDS):
        return "tactic"
    if any(sk in h for sk in SIGNAL_KEYWORDS):
        return "signal"
    return None


def _find_header_row(df: pd.DataFrame) -> int:
    """Return the index of the first row that looks like a header (has tactic/signal keywords)."""
    all_kw = TACTIC_KEYWORDS + SIGNAL_KEYWORDS
    for i in range(min(8, len(df))):
        row_str = " ".join(str(v).lower() for v in df.iloc[i].tolist())
        if any(kw in row_str for kw in all_kw):
            return i
    return -1


def detect_column_groups(df: pd.DataFrame) -> Tuple[Dict[str, Dict[str, int]], int]:
    """
    Detect column groups (tactic, subtactic, signal) per language.
    Returns ({language: {tactic: col_idx, subtactic: col_idx, signal: col_idx}}, header_row_idx)
    """
    header_row_idx = _find_header_row(df)
    if header_row_idx == -1:
        return {}, -1

    headers = [str(v).strip() for v in df.iloc[header_row_idx].tolist()]
    groups: Dict[str, Dict[str, int]] = {}

    for idx, h in enumerate(headers):
        if not h or h.lower() == "nan":
            continue
        col_t = _col_type(h)
        if col_t is None:
            continue
        lang = _detect_col_language(h)
        if lang not in groups:
            groups[lang] = {"tactic": -1, "subtactic": -1, "signal": -1}
        if groups[lang][col_t] == -1:
            groups[lang][col_t] = idx

    # Keep only groups that have at least tactic or signal detected
    groups = {k: v for k, v in groups.items() if v["tactic"] != -1 or v["signal"] != -1}
    return groups, header_row_idx


def read_context_rows(xls: Any, tab_name: str) -> Tuple[List[Dict[str, Any]], str]:
    """
    Read all context rows from the given sheet tab.
    Handles multilingual sheets (English + one local language).
    Returns (rows, local_language_name).
    Each row: {tactic_en, subtactic_en, signal_en, tactic_local, subtactic_local, signal_local, local_language}
    """
    df = pd.read_excel(xls, sheet_name=tab_name, header=None)
    groups, header_row_idx = detect_column_groups(df)

    if not groups or header_row_idx == -1:
        return [], ""

    en_group    = groups.get("English")
    # First non-English language group
    local_group = next((v for k, v in groups.items() if k != "English"), None)
    local_lang  = next((k for k in groups if k != "English"), "")

    # If only one group found, treat it as English
    if not en_group and local_group:
        en_group    = local_group
        local_group = None
        local_lang  = ""

    if not en_group:
        return [], ""

    def cell(row: Any, group: Optional[Dict[str, int]], field: str) -> str:
        if group is None:
            return ""
        idx = group.get(field, -1)
        if idx == -1 or idx >= len(row):
            return ""
        v = row.iloc[idx]
        s = str(v).strip() if pd.notna(v) else ""
        return "" if s.lower() == "nan" else s

    rows: List[Dict[str, Any]] = []
    for i in range(header_row_idx + 1, len(df)):
        row = df.iloc[i]
        tactic_en = cell(row, en_group, "tactic")
        signal_en = cell(row, en_group, "signal")

        if not tactic_en or not signal_en:
            continue
        # Skip if the row is itself a header repeat
        if any(kw in tactic_en.lower() for kw in TACTIC_KEYWORDS):
            continue

        rows.append({
            "tactic_en":     tactic_en,
            "subtactic_en":  cell(row, en_group, "subtactic"),
            "signal_en":     signal_en,
            "tactic_local":  cell(row, local_group, "tactic"),
            "subtactic_local": cell(row, local_group, "subtactic"),
            "signal_local":  cell(row, local_group, "signal"),
            "local_language": local_lang,
        })

    return rows, local_lang


# ---------------------------------------------------------------------------
# DB writes
# ---------------------------------------------------------------------------

def insert_mdb_run(conn, run_id: str, started_at: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO mdb_runs(run_id, started_at_utc, status) VALUES (%s,%s,%s)",
            (run_id, started_at, "running"),
        )
    conn.commit()


def finalize_mdb_run(conn, run_id: str, finished_at: str, status: str,
                     processed: int, blocked: int, skipped: int, failed: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE mdb_runs
               SET finished_at_utc=%s, status=%s,
                   processed_count=%s, blocked_count=%s,
                   skipped_count=%s, failed_count=%s
               WHERE run_id=%s""",
            (finished_at, status, processed, blocked, skipped, failed, run_id),
        )
    conn.commit()


def already_processed(conn, item_id: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM context_rows WHERE monday_item_id=%s LIMIT 1",
            (str(item_id),),
        )
        return cur.fetchone() is not None


def insert_context_rows_batch(conn, run_id: str, meta: Dict[str, Any],
                               rows: List[Dict[str, Any]]) -> None:
    now = utc_now_iso()
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(
            cur,
            """INSERT INTO context_rows (
                 run_id, monday_item_id, monday_board_id, monday_url,
                 region, campaign_name, brand, country, vertical, brief,
                 derived_language,
                 tactic_en, subtactic_en, signal_en,
                 tactic_local, subtactic_local, signal_local, local_language,
                 inserted_at_utc
               ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            [
                (
                    run_id,
                    meta["item_id"], meta["board_id"], meta["monday_url"],
                    meta["region"], meta["campaign_name"], meta["brand"],
                    meta["country"], meta["vertical"], meta["brief"],
                    meta["derived_language"],
                    r["tactic_en"], r["subtactic_en"], r["signal_en"],
                    r["tactic_local"], r["subtactic_local"], r["signal_local"],
                    r["local_language"],
                    now,
                )
                for r in rows
            ],
        )
    conn.commit()


def upsert_access_blocked(conn, run_id: str, meta: Dict[str, Any], error: str) -> None:
    now = utc_now_iso()
    with conn.cursor() as cur:
        # Don't create a duplicate open entry for the same item
        cur.execute(
            "SELECT 1 FROM access_blocked WHERE monday_item_id=%s AND resolved_at_utc IS NULL LIMIT 1",
            (str(meta.get("item_id", "")),),
        )
        if cur.fetchone():
            # Update error message on existing open entry
            cur.execute(
                """UPDATE access_blocked SET error_message=%s, date_flagged_utc=%s, run_id=%s
                   WHERE monday_item_id=%s AND resolved_at_utc IS NULL""",
                (error, now, run_id, str(meta.get("item_id", ""))),
            )
            conn.commit()
            return
        cur.execute(
            """INSERT INTO access_blocked (
                 run_id, monday_item_id, monday_board_id, monday_url,
                 region, campaign_name, brand, country,
                 media_plan_url, error_message, date_flagged_utc
               ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
                run_id,
                meta.get("item_id"), meta.get("board_id"), meta.get("monday_url"),
                meta.get("region"), meta.get("campaign_name"), meta.get("brand"),
                meta.get("country"), meta.get("media_plan_url"), error, now,
            ),
        )
    conn.commit()


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def load_board_configs(config_path: str) -> List[Dict[str, Any]]:
    with open(config_path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    boards = []
    product_keys = [
        "products_to_pitch", "product_to_pitch", "platform_to_pitch",
        "product_to_propose", "products_to_propose",
        "product_proposed", "products_proposed",
    ]

    for b in raw.get("boards", []):
        cfg = b.get("column_id_map", {})
        product_col_ids = []
        for k in product_keys:
            v = cfg.get(k)
            if v and v not in product_col_ids:
                product_col_ids.append(v)

        boards.append({
            "region":          b["region"],
            "board_id":        str(b["board_id"]),
            "col_brand":       cfg.get("brand_name",       "text_mkqjgbdc"),
            "col_rfp":         cfg.get("rfp_summary",      "long_text_mkqjgtxh"),
            "col_targeting":   cfg.get("targeting"),
            "col_country":     cfg.get("country"),
            "col_vertical":    cfg.get("vertical"),
            "col_media_plan":  cfg.get("media_plan",       "text_mkqnx8ze"),
            "col_any_other":   cfg.get("any_other_details"),
            "col_run_dates":   cfg.get("run_dates"),
            "products":        product_col_ids,
        })
    return boards


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    if len(sys.argv) < 2:
        print(
            "Usage: python3 mdb_pipeline.py <monday_config.json> [--since YYYY-MM-DD]\n"
            "\n"
            "  --since   Only process campaigns updated on/after this date.\n"
            "            Defaults to 30 days ago.\n"
            "\nEnv vars required:\n"
            "  MONDAY_API_KEY\n"
            "  DATABASE_URL\n"
        )
        sys.exit(1)

    config_path = sys.argv[1]
    since_date: Optional[str] = None
    if "--since" in sys.argv:
        idx = sys.argv.index("--since")
        if idx + 1 < len(sys.argv):
            since_date = sys.argv[idx + 1]
    if not since_date:
        since_date = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")

    monday_key = _get_env("MONDAY_API_KEY")
    if not monday_key:
        raise RuntimeError("MONDAY_API_KEY is not set.")

    boards = load_board_configs(config_path)
    conn = get_db()
    init_mdb_db(conn)

    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    insert_mdb_run(conn, run_id, utc_now_iso())
    print(f"MDB Pipeline run: {run_id}  |  since: {since_date}")

    counts = {"processed": 0, "blocked": 0, "skipped": 0, "failed": 0}
    since_dt = datetime.fromisoformat(since_date).replace(tzinfo=timezone.utc)

    for board in boards:
        region   = board["region"]
        board_id = board["board_id"]
        print(f"\n── Board: {region}")

        col_ids = list(dict.fromkeys(filter(None, [
            board["col_brand"],  board["col_rfp"],
            board["col_targeting"], board["col_country"],
            board["col_vertical"], board["col_media_plan"],
            board.get("col_any_other"), board.get("col_run_dates"),
        ] + board["products"])))

        try:
            items = fetch_done_items(monday_key, board_id, col_ids)
        except Exception as e:
            print(f"   ERROR fetching board: {e}")
            counts["failed"] += 1
            continue

        # Filter by date
        recent = []
        for item in items:
            ts = item.get("updated_at") or item.get("created_at") or ""
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                if dt >= since_dt:
                    recent.append(item)
            except Exception:
                recent.append(item)

        print(f"   {len(items)} Done items — {len(recent)} on/after {since_date}")

        for item in recent:
            item_id       = str(item["id"])
            campaign_name = item.get("name", "") or ""
            monday_url    = MONDAY_ITEM_URL.format(board_id=board_id, item_id=item_id)

            col_values: Dict[str, str] = {
                cv["id"]: _format_col_value(cv)
                for cv in item.get("column_values", [])
            }

            if already_processed(conn, item_id):
                print(f"   SKIP (already done): {campaign_name}")
                counts["skipped"] += 1
                continue

            if not should_include_campaign(col_values, board):
                print(f"   SKIP (no Mirror/YouTube): {campaign_name}")
                counts["skipped"] += 1
                continue

            brand          = col_values.get(board["col_brand"],         "") or ""
            country        = col_values.get(board["col_country"] or "",  "") or ""
            vertical       = col_values.get(board["col_vertical"] or "", "") or ""
            rfp            = col_values.get(board["col_rfp"],            "") or ""
            targeting      = col_values.get(board["col_targeting"] or "", "") or ""
            any_other      = col_values.get(board.get("col_any_other") or "", "") or ""
            media_plan_url = col_values.get(board["col_media_plan"],     "") or ""
            brief          = " | ".join(filter(None, [rfp, targeting, any_other]))

            meta = {
                "item_id":       item_id,
                "board_id":      board_id,
                "monday_url":    monday_url,
                "region":        region,
                "campaign_name": campaign_name,
                "brand":         brand,
                "country":       country,
                "vertical":      vertical,
                "brief":         brief,
                "derived_language": "",
                "media_plan_url": media_plan_url,
            }

            if not media_plan_url:
                print(f"   SKIP (no media plan link): {campaign_name}")
                counts["skipped"] += 1
                continue

            print(f"   Processing: {campaign_name}")

            # Read the media plan Google Sheet
            try:
                xls = read_public_sheet(media_plan_url)
            except PermissionError as e:
                print(f"   BLOCKED: {e}")
                upsert_access_blocked(conn, run_id, meta, str(e))
                counts["blocked"] += 1
                continue
            except Exception as e:
                print(f"   FAILED (sheet read error): {e}")
                upsert_access_blocked(conn, run_id, meta, str(e))
                counts["failed"] += 1
                continue

            # Find context tab
            tab_name = find_context_tab(xls)
            if not tab_name:
                msg = "No 'context' tab found in media plan"
                print(f"   FAILED: {msg}")
                upsert_access_blocked(conn, run_id, meta, msg)
                counts["failed"] += 1
                continue

            # Extract context rows
            try:
                context_rows, local_lang = read_context_rows(xls, tab_name)
            except Exception as e:
                msg = f"Error reading context tab: {e}"
                print(f"   FAILED: {msg}")
                upsert_access_blocked(conn, run_id, meta, msg)
                counts["failed"] += 1
                continue

            if not context_rows:
                msg = "Context tab found but contains no valid rows"
                print(f"   FAILED: {msg}")
                upsert_access_blocked(conn, run_id, meta, msg)
                counts["failed"] += 1
                continue

            meta["derived_language"] = local_lang
            insert_context_rows_batch(conn, run_id, meta, context_rows)
            print(f"   ✅ {campaign_name} — {len(context_rows)} rows"
                  f"{f' (local: {local_lang})' if local_lang else ''}")
            counts["processed"] += 1

    finalize_mdb_run(
        conn, run_id, utc_now_iso(), "success",
        counts["processed"], counts["blocked"], counts["skipped"], counts["failed"],
    )
    conn.close()

    print(
        f"\n✅ MDB Pipeline complete — Run: {run_id}\n"
        f"   Processed: {counts['processed']}  |  Blocked: {counts['blocked']}"
        f"  |  Skipped: {counts['skipped']}  |  Failed: {counts['failed']}"
    )


if __name__ == "__main__":
    main()
