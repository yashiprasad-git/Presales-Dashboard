import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    psycopg2 = None  # type: ignore

MDB_PIPELINE_SCRIPT = Path(__file__).resolve().parent / "mdb_pipeline.py"


APP_TITLE = "Presales Inventory Dashboard"
PRESALES_DIR = Path(__file__).resolve().parent

RECOMMENDATIONS_GLOB = "Monday_Presales_Recommendations_*.xlsx"
PIPELINE_SCRIPT = PRESALES_DIR / "monday_presales_pipeline.py"

LOW_STATUSES = {"Nil", "Low", "Medium"}


# ---------------------------------------------------------------------------
# Database connection (PostgreSQL via DATABASE_URL)
# ---------------------------------------------------------------------------

def _get_database_url() -> str:
    url = os.getenv("DATABASE_URL") or st.secrets.get("DATABASE_URL", "")  # type: ignore[attr-defined]
    if not url:
        raise RuntimeError(
            "DATABASE_URL is not set. Add it to your environment or .streamlit/secrets.toml."
        )
    return url


def get_db():
    """Open a fresh short-lived connection. Use for pipeline runs and mutations."""
    if psycopg2 is None:
        raise RuntimeError("psycopg2 is not installed. Run: pip install psycopg2-binary")
    url = _get_database_url()
    conn = psycopg2.connect(url)
    conn.autocommit = False
    return conn


@st.cache_resource(show_spinner=False)
def _ensure_schema_initialized() -> bool:
    """
    Run CREATE TABLE / ALTER TABLE / CREATE INDEX exactly once per server session.
    Returns True so callers can check it was done. The connection used here is
    closed immediately after — dashboard code always opens its own fresh connection.
    """
    conn = get_db()
    try:
        init_db(conn)
        init_mdb_db(conn)
    finally:
        conn.close()
    return True


def get_dashboard_conn():
    """
    Return a fresh DB connection for use during a single Streamlit render cycle.
    Schema is guaranteed to be initialised (runs only once per server session).
    Caller is responsible for closing the connection.
    """
    _ensure_schema_initialized()
    return get_db()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ---------------------------------------------------------------------------
# Schema init (idempotent)
# ---------------------------------------------------------------------------

def init_db(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline_runs (
              run_id TEXT PRIMARY KEY,
              started_at_utc TEXT NOT NULL,
              finished_at_utc TEXT,
              status TEXT NOT NULL,
              stdout TEXT,
              stderr TEXT,
              recommendations_file TEXT
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS results (
              id SERIAL PRIMARY KEY,
              run_id TEXT NOT NULL,
              region TEXT NOT NULL,
              campaign_name TEXT,
              brand_name TEXT,
              vertical TEXT,
              country TEXT,
              derived_language TEXT,
              products_to_pitch TEXT,
              monday_run_dates TEXT,
              monday_submitted_at_utc TEXT,
              recommended_category TEXT,
              available_inventory_count INTEGER,
              p1_channel_count INTEGER,
              p2_channel_count INTEGER,
              p3_channel_count INTEGER,
              inventory_status TEXT,
              error_log TEXT,
              inserted_at_utc TEXT NOT NULL
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS alerts (
              alert_id SERIAL PRIMARY KEY,
              region TEXT NOT NULL,
              campaign_name TEXT,
              brand_name TEXT,
              country TEXT,
              derived_language TEXT,
              products_to_pitch TEXT,
              monday_run_dates TEXT,
              monday_submitted_at_utc TEXT,
              recommended_category TEXT,
              inventory_status TEXT NOT NULL,
              p1_channel_count INTEGER,
              p2_channel_count INTEGER,
              p3_channel_count INTEGER,
              available_inventory_count INTEGER,
              error_log TEXT,
              date_flagged_utc TEXT NOT NULL,
              resolved_at_utc TEXT,
              resolved_by TEXT,
              resolved_note TEXT
            );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_alerts_open
            ON alerts(resolved_at_utc, region, inventory_status);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_results_run
            ON results(run_id, region);
            """
        )
        # Add monday_url column to existing tables if not present
        cur.execute("ALTER TABLE results ADD COLUMN IF NOT EXISTS monday_url TEXT;")
        cur.execute("ALTER TABLE alerts  ADD COLUMN IF NOT EXISTS monday_url TEXT;")
    conn.commit()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def latest_recommendations_file() -> Optional[Path]:
    matches = list(PRESALES_DIR.glob(RECOMMENDATIONS_GLOB))
    if not matches:
        return None
    return max(matches, key=lambda p: p.stat().st_mtime)


def safe_int(v: Any) -> Optional[int]:
    try:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return None
        return int(v)
    except Exception:
        return None


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


def cell_str(val: Any) -> str:
    if val is None:
        return ""
    try:
        if pd.isna(val):
            return ""
    except Exception:
        pass
    s = str(val).strip()
    return "" if s.lower() == "nan" else s


def _clean_products(df: pd.DataFrame) -> pd.DataFrame:
    if "products_to_pitch" in df.columns:
        df["products_to_pitch"] = (
            df["products_to_pitch"]
            .astype(str)
            .replace({"nan": "", "NaN": "", "None": ""})
            .fillna("")
        )
    return df


# ---------------------------------------------------------------------------
# Workbook extraction
# ---------------------------------------------------------------------------

def extract_rows_from_workbook(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    xls = pd.ExcelFile(path)
    for sheet in xls.sheet_names:
        df = normalize_columns(pd.read_excel(xls, sheet_name=sheet))
        region = sheet

        def col(*names: str) -> Optional[str]:
            lower_map = {c.lower(): c for c in df.columns}
            for n in names:
                c = lower_map.get(n.lower())
                if c:
                    return c
            return None

        c_name = col("Name", "Campaign", "Campaign Name")
        c_brand = col("Brand Name", "Brand")
        c_vertical = col("Vertical")
        c_country = col("Country where campaign will run", "Country")
        c_products = col("Products to Pitch", "Product Proposed", "Product to propose", "Product To Propose")
        c_run_dates = col("Run dates", "Run Dates", "Run date", "Run Date")
        c_lang = col("Derived_Language", "Derived Language")
        c_submitted = col("Monday_Submitted_At", "Monday Submitted At", "Submitted At", "Created At")
        c_cats = col("Recommended_Category", "Recommended Category")
        c_total = col("Available_Inventory_Count", "Available Inventory Count")
        c_p1 = col("P1_Channel_Count", "P1 Channel Count")
        c_p2 = col("P2_Channel_Count", "P2 Channel Count")
        c_p3 = col("P3_Channel_Count", "P3 Channel Count")
        c_status = col("Inventory_Status", "Inventory Status")
        c_err = col("Error_Log", "Error Log")
        c_monday_url = col("Monday_URL", "Monday URL")

        for _, r in df.iterrows():
            status = (cell_str(r.get(c_status, "")) if c_status else "").strip()
            if not status:
                continue
            rows.append(
                {
                    "region": region,
                    "campaign_name": cell_str(r.get(c_name, "")) if c_name else "",
                    "brand_name": cell_str(r.get(c_brand, "")) if c_brand else "",
                    "vertical": cell_str(r.get(c_vertical, "")) if c_vertical else "",
                    "country": cell_str(r.get(c_country, "")) if c_country else "",
                    "products_to_pitch": cell_str(r.get(c_products, "")) if c_products else "",
                    "monday_run_dates": cell_str(r.get(c_run_dates, "")) if c_run_dates else "",
                    "derived_language": cell_str(r.get(c_lang, "")) if c_lang else "",
                    "monday_submitted_at_utc": cell_str(r.get(c_submitted, "")) if c_submitted else "",
                    "recommended_category": cell_str(r.get(c_cats, "")) if c_cats else "",
                    "available_inventory_count": safe_int(r.get(c_total)) if c_total else None,
                    "p1_channel_count": safe_int(r.get(c_p1)) if c_p1 else None,
                    "p2_channel_count": safe_int(r.get(c_p2)) if c_p2 else None,
                    "p3_channel_count": safe_int(r.get(c_p3)) if c_p3 else None,
                    "inventory_status": status,
                    "error_log": cell_str(r.get(c_err, "")) if c_err else "",
                    "monday_url": cell_str(r.get(c_monday_url, "")) if c_monday_url else "",
                }
            )
    return rows


# ---------------------------------------------------------------------------
# DB writes
# ---------------------------------------------------------------------------

def insert_run(conn, run_id: str, started_at: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO pipeline_runs(run_id, started_at_utc, status) VALUES (%s, %s, %s)",
            (run_id, started_at, "running"),
        )
    conn.commit()


def finalize_run(conn, run_id: str, finished_at: str, status: str,
                 stdout: str, stderr: str, recommendations_file: Optional[str]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pipeline_runs
            SET finished_at_utc = %s, status = %s, stdout = %s, stderr = %s, recommendations_file = %s
            WHERE run_id = %s
            """,
            (finished_at, status, stdout, stderr, recommendations_file, run_id),
        )
    conn.commit()


def insert_results(conn, run_id: str, rows: List[Dict[str, Any]]) -> None:
    now = utc_now_iso()
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(
            cur,
            """
            INSERT INTO results(
              run_id, region, campaign_name, brand_name, vertical, country, derived_language,
              products_to_pitch, monday_run_dates, monday_submitted_at_utc, recommended_category,
              available_inventory_count, p1_channel_count, p2_channel_count, p3_channel_count,
              inventory_status, error_log, monday_url, inserted_at_utc
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            [
                (
                    run_id, r["region"], r.get("campaign_name"), r.get("brand_name"),
                    r.get("vertical"), r.get("country"), r.get("derived_language"),
                    r.get("products_to_pitch"), r.get("monday_run_dates"),
                    r.get("monday_submitted_at_utc"), r.get("recommended_category"),
                    r.get("available_inventory_count"), r.get("p1_channel_count"),
                    r.get("p2_channel_count"), r.get("p3_channel_count"),
                    r.get("inventory_status"), r.get("error_log"), r.get("monday_url"), now,
                )
                for r in rows
            ],
        )
    conn.commit()


def open_alert_exists(conn, r: Dict[str, Any]) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM alerts
            WHERE resolved_at_utc IS NULL
              AND region = %s
              AND COALESCE(campaign_name,'') = %s
              AND COALESCE(country,'') = %s
              AND inventory_status = %s
            LIMIT 1
            """,
            (r["region"], r.get("campaign_name", ""), r.get("country", ""), r.get("inventory_status", "")),
        )
        return cur.fetchone() is not None


def insert_alerts(conn, rows: List[Dict[str, Any]]) -> int:
    now = utc_now_iso()
    inserted = 0
    for r in rows:
        status = str(r.get("inventory_status", "")).strip()
        if status not in LOW_STATUSES:
            continue
        if open_alert_exists(conn, r):
            continue
        with conn.cursor() as cur:
                cur.execute(
                """
                INSERT INTO alerts(
                  region, campaign_name, brand_name, country, derived_language,
                  products_to_pitch, monday_run_dates, monday_submitted_at_utc, recommended_category,
                  inventory_status, p1_channel_count, p2_channel_count, p3_channel_count,
                  available_inventory_count, error_log, monday_url, date_flagged_utc
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    r["region"], r.get("campaign_name"), r.get("brand_name"), r.get("country"),
                    r.get("derived_language"), r.get("products_to_pitch"), r.get("monday_run_dates"),
                    r.get("monday_submitted_at_utc"), r.get("recommended_category"),
                    status, r.get("p1_channel_count"), r.get("p2_channel_count"),
                    r.get("p3_channel_count"), r.get("available_inventory_count"),
                    r.get("error_log"), r.get("monday_url"), now,
                ),
            )
        inserted += 1
    conn.commit()
    return inserted


# ---------------------------------------------------------------------------
# Pipeline runner
# ---------------------------------------------------------------------------

FIRST_RUN_SINCE = "2026-03-20"  # Hard floor — never process campaigns created before this date


def _last_successful_run_date(conn) -> Optional[str]:
    """Return the started_at_utc of the last successful pipeline run, or None."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT started_at_utc FROM pipeline_runs WHERE status = 'success' "
            "ORDER BY started_at_utc DESC LIMIT 1"
        )
        row = cur.fetchone()
    if row and row[0]:
        return str(row[0])[:10]
    return None


def _processed_item_ids(conn) -> List[str]:
    """Return all Monday item IDs that have already been processed (stored in results)."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT monday_item_id FROM results "
            "WHERE monday_item_id IS NOT NULL AND monday_item_id <> ''"
        )
        rows = cur.fetchall()
    return [str(r[0]) for r in rows if r[0]]


def run_pipeline_and_ingest(config_path: Path, inventory_path: Path) -> Tuple[str, str]:
    if not PIPELINE_SCRIPT.exists():
        raise RuntimeError(f"Missing pipeline script: {PIPELINE_SCRIPT}")

    monday_key = os.getenv("MONDAY_API_KEY") or st.secrets.get("MONDAY_API_KEY", "")  # type: ignore[attr-defined]
    openai_key = os.getenv("OPENAI_API_KEY") or st.secrets.get("OPENAI_API_KEY", "")  # type: ignore[attr-defined]
    if not monday_key:
        raise RuntimeError("MONDAY_API_KEY is not set.")
    if not openai_key:
        raise RuntimeError("OPENAI_API_KEY is not set.")

    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    started_at = utc_now_iso()

    # ── Pre-run DB work (quick queries — close connection before the long subprocess) ──
    conn = get_db()
    since_date = _last_successful_run_date(conn) or FIRST_RUN_SINCE
    skip_ids = _processed_item_ids(conn)
    insert_run(conn, run_id, started_at)
    conn.close()   # <-- close NOW so Supabase doesn't time out during the pipeline run

    # Write already-processed item IDs to a temp file so the pipeline can skip them
    skip_ids_file = tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False, dir=str(PRESALES_DIR)
    )
    json.dump(skip_ids, skip_ids_file)
    skip_ids_file.close()
    skip_ids_path = skip_ids_file.name

    before = set(p.name for p in PRESALES_DIR.glob(RECOMMENDATIONS_GLOB))

    env = os.environ.copy()
    env["MONDAY_API_KEY"] = monday_key
    env["OPENAI_API_KEY"] = openai_key

    proc = subprocess.run(
        [sys.executable, str(PIPELINE_SCRIPT), str(config_path), str(inventory_path),
         "--since", since_date, "--skip-ids-file", skip_ids_path],
        cwd=str(PRESALES_DIR),
        capture_output=True,
        text=True,
        env=env,
    )

    # Clean up the skip-IDs temp file
    try:
        Path(skip_ids_path).unlink(missing_ok=True)
    except Exception:
        pass

    after = list(PRESALES_DIR.glob(RECOMMENDATIONS_GLOB))
    new_files = [p for p in after if p.name not in before]
    rec_file = max(new_files, key=lambda p: p.stat().st_mtime) if new_files else latest_recommendations_file()

    finished_at = utc_now_iso()
    status = "success" if proc.returncode == 0 else "failed"

    # ── Post-run DB work — fresh connection after subprocess completes ──────
    conn = get_db()
    finalize_run(conn, run_id, finished_at, status, proc.stdout or "", proc.stderr or "",
                 str(rec_file) if rec_file else None)

    if status != "success":
        conn.close()
        raise RuntimeError(proc.stderr or proc.stdout or "Pipeline failed (no output).")
    if not rec_file or not rec_file.exists():
        conn.close()
        raise RuntimeError("Pipeline succeeded but no recommendations file was found.")

    rows = extract_rows_from_workbook(rec_file)
    insert_results(conn, run_id, rows)
    insert_alerts(conn, rows)
    conn.close()

    # Also run MDB pipeline (context rows) — best effort, don't fail the main run
    if MDB_PIPELINE_SCRIPT.exists():
        try:
            mdb_env = env.copy()
            subprocess.run(
                [sys.executable, str(MDB_PIPELINE_SCRIPT), str(config_path)],
                cwd=str(PRESALES_DIR), capture_output=True, text=True, env=mdb_env,
                timeout=300,
            )
        except Exception:
            pass  # MDB pipeline failure should not block the main pipeline result

    return run_id, status


# ---------------------------------------------------------------------------
# DB reads
# ---------------------------------------------------------------------------

def _df_from_query(conn, sql: str, params: tuple = ()) -> pd.DataFrame:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params or None)
        rows = cur.fetchall()
    return pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()


def fetch_open_alerts(conn) -> pd.DataFrame:
    df = _df_from_query(
        conn,
        """
        SELECT alert_id, region, campaign_name, brand_name, country, derived_language,
               products_to_pitch, monday_run_dates, monday_submitted_at_utc, recommended_category,
               inventory_status, p1_channel_count, p2_channel_count, p3_channel_count,
               available_inventory_count, error_log, monday_url, date_flagged_utc
        FROM alerts
        WHERE resolved_at_utc IS NULL
        ORDER BY date_flagged_utc DESC
        """,
    )
    return _clean_products(df)


def fetch_resolved_alerts(conn) -> pd.DataFrame:
    df = _df_from_query(
        conn,
        """
        SELECT alert_id, region, campaign_name, brand_name, country, derived_language,
               products_to_pitch, monday_run_dates, monday_submitted_at_utc, recommended_category,
               inventory_status, p1_channel_count, p2_channel_count, p3_channel_count,
               available_inventory_count, error_log, monday_url, date_flagged_utc,
               resolved_at_utc, resolved_by, resolved_note
        FROM alerts
        WHERE resolved_at_utc IS NOT NULL
        ORDER BY resolved_at_utc DESC
        """,
    )
    return _clean_products(df)


def fetch_all_results(conn, days: Optional[int] = 30, older: bool = False) -> pd.DataFrame:
    cutoff_iso: Optional[str] = None
    params: tuple = ()
    if days is not None:
        cutoff = datetime.now(timezone.utc) - pd.Timedelta(days=days)
        cutoff_iso = cutoff.isoformat(timespec="seconds")

    if cutoff_iso:
        date_clause = "WHERE r.inserted_at_utc < %s" if older else "WHERE r.inserted_at_utc >= %s"
        params = (cutoff_iso,)
    else:
        date_clause = ""

    df = _df_from_query(
        conn,
        f"""
        SELECT run_inserted_at_utc, run_id, region, campaign_name, brand_name, vertical, country,
               derived_language, products_to_pitch, monday_run_dates, recommended_category,
               p1_channel_count, p2_channel_count, p3_channel_count, available_inventory_count,
               inventory_status, error_log, monday_url
        FROM (
          SELECT r.inserted_at_utc AS run_inserted_at_utc, r.run_id, r.region, r.campaign_name,
                 r.brand_name, r.vertical, r.country, r.derived_language, r.products_to_pitch,
                 r.monday_run_dates, r.recommended_category, r.p1_channel_count, r.p2_channel_count,
                 r.p3_channel_count, r.available_inventory_count, r.inventory_status, r.error_log, r.monday_url,
                 ROW_NUMBER() OVER (
                   PARTITION BY r.region, COALESCE(r.campaign_name,''), COALESCE(r.brand_name,'')
                   ORDER BY r.inserted_at_utc DESC
                 ) AS rn
          FROM results r
          {date_clause}
        ) t
        WHERE rn = 1
        ORDER BY run_inserted_at_utc DESC
        """,
        params,
    )
    return _clean_products(df)


def init_mdb_db(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS mdb_runs (
              run_id TEXT PRIMARY KEY, started_at_utc TEXT NOT NULL,
              finished_at_utc TEXT, status TEXT NOT NULL,
              processed_count INTEGER DEFAULT 0, blocked_count INTEGER DEFAULT 0,
              skipped_count INTEGER DEFAULT 0, failed_count INTEGER DEFAULT 0
            );""")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS context_rows (
              id SERIAL PRIMARY KEY, run_id TEXT, monday_item_id TEXT,
              monday_board_id TEXT, monday_url TEXT, region TEXT,
              campaign_name TEXT, brand TEXT, country TEXT, vertical TEXT,
              brief TEXT, derived_language TEXT,
              tactic_en TEXT, subtactic_en TEXT, signal_en TEXT,
              tactic_local TEXT, subtactic_local TEXT, signal_local TEXT,
              local_language TEXT, inserted_at_utc TEXT NOT NULL
            );""")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS access_blocked (
              id SERIAL PRIMARY KEY, run_id TEXT, monday_item_id TEXT,
              monday_board_id TEXT, monday_url TEXT, region TEXT,
              campaign_name TEXT, brand TEXT, country TEXT,
              media_plan_url TEXT, error_message TEXT,
              date_flagged_utc TEXT NOT NULL,
              resolved_at_utc TEXT, resolved_by TEXT, resolved_note TEXT
            );""")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_context_campaign ON context_rows(region, campaign_name);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_blocked_open ON access_blocked(resolved_at_utc, region);")
    conn.commit()


def last_updated(conn) -> Optional[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(inserted_at_utc) FROM results")
        row = cur.fetchone()
    return row[0] if row and row[0] else None



def fetch_context_rows(conn) -> pd.DataFrame:
    df = _df_from_query(
        conn,
        """SELECT region, campaign_name, brand, country, vertical,
                  monday_url, derived_language, local_language,
                  tactic_en, subtactic_en, signal_en,
                  tactic_local, subtactic_local, signal_local,
                  inserted_at_utc
           FROM context_rows
           ORDER BY inserted_at_utc DESC""",
    )
    return df.fillna("") if not df.empty else df


def fetch_access_blocked(conn) -> pd.DataFrame:
    df = _df_from_query(
        conn,
        """SELECT id, region, campaign_name, brand, country,
                  monday_url, media_plan_url, error_message,
                  date_flagged_utc, resolved_at_utc, resolved_by, resolved_note
           FROM access_blocked
           WHERE resolved_at_utc IS NULL
           ORDER BY date_flagged_utc DESC""",
    )
    return df.fillna("") if not df.empty else df


def resolve_blocked(conn, blocked_id: int, resolved_by: str = "", note: str = "") -> None:
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE access_blocked
               SET resolved_at_utc=%s, resolved_by=%s, resolved_note=%s
               WHERE id=%s""",
            (utc_now_iso(), resolved_by.strip() or None, note.strip() or None, blocked_id),
        )
    conn.commit()


def resolve_alert(conn, alert_id: int, resolved_by: str = "", note: str = "") -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE alerts
            SET resolved_at_utc = %s, resolved_by = %s, resolved_note = %s
            WHERE alert_id = %s
            """,
            (utc_now_iso(), resolved_by.strip() or None, note.strip() or None, alert_id),
        )
    conn.commit()


# ---------------------------------------------------------------------------
# Streamlit UI
# ---------------------------------------------------------------------------

def main() -> None:
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)

    # Fresh connection per render; schema init runs only once per server session
    conn = get_dashboard_conn()

    if "resolve_alert_id" not in st.session_state:
        st.session_state["resolve_alert_id"] = None

    updated = last_updated(conn)
    st.caption(f"Last updated: **{updated or 'Never'}** (UTC)")

    with st.sidebar:
        st.subheader("Pipeline Inputs")
        config_path = st.text_input(
            "Monday config JSON path",
            value=str(PRESALES_DIR / "monday_config.json"),
        )
        inventory_path = st.text_input(
            "Inventory file path",
            value=str(PRESALES_DIR / "Inventory.xlsx"),
        )

        st.divider()
        st.subheader("Environment")
        st.write("`MONDAY_API_KEY`:", "set" if (os.getenv("MONDAY_API_KEY") or st.secrets.get("MONDAY_API_KEY")) else "missing")  # type: ignore[attr-defined]
        st.write("`OPENAI_API_KEY`:", "set" if (os.getenv("OPENAI_API_KEY") or st.secrets.get("OPENAI_API_KEY")) else "missing")  # type: ignore[attr-defined]
        st.write("`DATABASE_URL`:", "set" if (os.getenv("DATABASE_URL") or st.secrets.get("DATABASE_URL")) else "missing")  # type: ignore[attr-defined]

        st.divider()
        run_clicked = st.button("Run Pipeline", type="primary", use_container_width=True)

    if run_clicked:
        try:
            # Preview the since-date before spinning
            _since_preview = _last_successful_run_date(conn) or FIRST_RUN_SINCE
            with st.spinner(
                f"Running pipeline… picking up 'Done' campaigns created on/after **{_since_preview}**. "
                "This can take a few minutes."
            ):
                run_id, _ = run_pipeline_and_ingest(Path(config_path), Path(inventory_path))
            st.success(f"Pipeline run completed. Run ID: {run_id}")
            st.rerun()
        except Exception as e:
            st.error(str(e))

    tab_alerts, tab_resolved, tab_all_30d, tab_all_old, tab_blocked = st.tabs(
        ["Open Alerts", "Resolved Alerts", "All Results (Last 30 days)",
         "Older Campaigns", "Access Blocked"]
    )

    with tab_alerts:
        df = fetch_open_alerts(conn)

        regions = ["All"] + sorted([r for r in df["region"].dropna().unique().tolist() if r]) if not df.empty else ["All"]
        statuses = ["All"] + sorted([s for s in df["inventory_status"].dropna().unique().tolist() if s]) if not df.empty else ["All"]

        c1, c2 = st.columns(2)
        with c1:
            region_filter = st.selectbox("Filter by Region", regions, index=0)
        with c2:
            status_filter = st.selectbox("Filter by Status", statuses, index=0)

        filtered = df.copy()
        if not filtered.empty:
            if region_filter != "All":
                filtered = filtered[filtered["region"] == region_filter]
            if status_filter != "All":
                filtered = filtered[filtered["inventory_status"] == status_filter]
        filtered = filtered.fillna("")

        st.write(f"Open alerts: **{len(filtered)}**")

        if filtered.empty:
            st.info("No open alerts.")
        else:
            widths = [1.8, 1.8, 1.8, 1.8, 2.2, 2.8, 1.5, 1, 1.6]
            header = st.columns(widths)
            header[0].markdown("**Region**")
            header[1].markdown("**Brand**")
            header[2].markdown("**Country**")
            header[3].markdown("**Derived Language**")
            header[4].markdown("**Run dates (Monday)**")
            header[5].markdown("**Recommended Category**")
            header[6].markdown("**Status**")
            header[7].markdown("**Campaign**")
            header[8].markdown("**Resolve**")

            row_by_id = {int(r["alert_id"]): r for _, r in filtered.iterrows()}

            for _, row in filtered.iterrows():
                alert_id = int(row["alert_id"])
                cols = st.columns(widths)
                cols[0].write(row.get("region", ""))
                cols[1].write(row.get("brand_name", ""))
                cols[2].write(row.get("country", ""))
                cols[3].write(row.get("derived_language", ""))
                cols[4].write(row.get("monday_run_dates", "") or "")
                cols[5].write(row.get("recommended_category", ""))
                cols[6].write(row.get("inventory_status", ""))
                m_url = row.get("monday_url", "") or ""
                if m_url:
                    cols[7].markdown(f"[Open]({m_url})")
                else:
                    cols[7].write("—")

                if cols[8].button("Resolve", key=f"open_resolve_{alert_id}", use_container_width=True):
                    st.session_state["resolve_alert_id"] = alert_id
                    st.rerun()

            target_id = st.session_state.get("resolve_alert_id")
            if target_id:
                target_row = row_by_id.get(int(target_id))
                if target_row is None:
                    st.session_state["resolve_alert_id"] = None
                else:
                    with st.container(border=True):
                        st.subheader("Resolve alert")
                        st.caption(
                            f"Campaign: {target_row.get('campaign_name','') or ''} | "
                            f"Brand: {target_row.get('brand_name','') or ''} | "
                            f"Region: {target_row.get('region','') or ''} | "
                            f"Country: {target_row.get('country','') or ''} | "
                            f"Status: {target_row.get('inventory_status','') or ''}"
                        )
                        resolver = st.text_input("Your name", key=f"resolved_by_panel_{target_id}")
                        default_note = (
                            "Current inventory for this alert:\n"
                            f"- Available_Inventory_Count: {target_row.get('available_inventory_count','')}\n"
                            f"- P1_Channel_Count: {target_row.get('p1_channel_count','')}\n"
                            f"- P2_Channel_Count: {target_row.get('p2_channel_count','')}\n"
                            f"- P3_Channel_Count: {target_row.get('p3_channel_count','')}\n"
                        )
                        note = st.text_area(
                            "Resolution note (required)",
                            value=default_note,
                            key=f"resolved_note_panel_{target_id}",
                            height=140,
                        )
                        c_ok, c_cancel = st.columns([1, 1])
                        with c_ok:
                            if st.button("Confirm resolve", type="primary", use_container_width=True):
                                if not resolver.strip():
                                    st.error("Please enter your name.")
                                elif not note.strip():
                                    st.error("Please enter a resolution note.")
                                else:
                                    resolve_alert(conn, int(target_id), resolved_by=resolver, note=note)
                                    st.session_state["resolve_alert_id"] = None
                                    st.rerun()
                        with c_cancel:
                            if st.button("Cancel", use_container_width=True):
                                st.session_state["resolve_alert_id"] = None
                                st.rerun()

    with tab_resolved:
        df_r = fetch_resolved_alerts(conn)
        if df_r.empty:
            st.info("No resolved alerts yet.")
        else:
            df_r = df_r.fillna("")
            st.write(f"Resolved alerts: **{len(df_r)}**")
            res_col_cfg = {}
            if "monday_url" in df_r.columns:
                res_col_cfg["monday_url"] = st.column_config.LinkColumn("Monday Link", display_text="Open")
            st.dataframe(df_r, use_container_width=True, height=600, column_config=res_col_cfg)

    with tab_all_30d:
        df_all = fetch_all_results(conn, days=30, older=False)
        if df_all.empty:
            st.info("No results in the last 30 days. Click **Run Pipeline** in the sidebar to generate results.")
        else:
            df_all = df_all.fillna("")
            regions_all = ["All"] + sorted([r for r in df_all["region"].dropna().unique().tolist() if r])
            statuses_all = ["All"] + sorted([s for s in df_all["inventory_status"].dropna().unique().tolist() if s])

            f1, f2, f3 = st.columns([1, 1, 2])
            with f1:
                region_filter_all = st.selectbox("Filter by Region", regions_all, index=0, key="all_region")
            with f2:
                status_filter_all = st.selectbox("Filter by Status", statuses_all, index=0, key="all_status")
            with f3:
                search = st.text_input(
                    "Search (Brand / Campaign / Category / Country)",
                    value="", key="all_search",
                ).strip().lower()

            filtered_all = df_all.copy()
            if region_filter_all != "All":
                filtered_all = filtered_all[filtered_all["region"] == region_filter_all]
            if status_filter_all != "All":
                filtered_all = filtered_all[filtered_all["inventory_status"] == status_filter_all]
            if search:
                haystack = (
                    filtered_all[["campaign_name", "brand_name", "country", "recommended_category"]]
                    .fillna("").astype(str).agg(" ".join, axis=1).str.lower()
                )
                filtered_all = filtered_all[haystack.str.contains(search, na=False)]

            st.write(f"Rows shown: **{len(filtered_all)}** (Total history: **{len(df_all)}**)")
            col_cfg = {}
            if "monday_url" in filtered_all.columns:
                col_cfg["monday_url"] = st.column_config.LinkColumn("Monday Link", display_text="Open")
            st.dataframe(filtered_all, use_container_width=True, height=600, column_config=col_cfg)

    with tab_all_old:
        df_old = fetch_all_results(conn, days=30, older=True)
        if df_old.empty:
            st.info("No older campaigns found.")
        else:
            df_old = df_old.fillna("")
            st.write(f"Older campaigns (older than 30 days): **{len(df_old)}**")
            old_col_cfg = {}
            if "monday_url" in df_old.columns:
                old_col_cfg["monday_url"] = st.column_config.LinkColumn("Monday Link", display_text="Open")
            st.dataframe(df_old, use_container_width=True, height=600, column_config=old_col_cfg)

    with tab_blocked:
        df_blk = fetch_access_blocked(conn)
        st.write(f"Access blocked (open): **{len(df_blk)}**")

        if df_blk.empty:
            st.info("No access-blocked media plans. All sheets were readable.")
        else:
            if "resolve_blocked_id" not in st.session_state:
                st.session_state["resolve_blocked_id"] = None

            blk_widths = [2, 2, 2, 3, 2, 1.5]
            blk_header = st.columns(blk_widths)
            blk_header[0].markdown("**Region**")
            blk_header[1].markdown("**Campaign**")
            blk_header[2].markdown("**Brand**")
            blk_header[3].markdown("**Error**")
            blk_header[4].markdown("**Flagged**")
            blk_header[5].markdown("**Resolve**")

            blk_by_id = {int(r["id"]): r for _, r in df_blk.iterrows()}

            for _, row in df_blk.iterrows():
                bid = int(row["id"])
                cols = st.columns(blk_widths)
                cols[0].write(row.get("region", ""))
                name_cell = row.get("campaign_name", "") or ""
                url = row.get("monday_url", "") or ""
                cols[1].markdown(f'[{name_cell}]({url})' if url else name_cell)
                cols[2].write(row.get("brand", ""))
                cols[3].write(str(row.get("error_message", ""))[:120])
                cols[4].write(str(row.get("date_flagged_utc", ""))[:10])
                if cols[5].button("Resolve", key=f"blk_resolve_{bid}", use_container_width=True):
                    st.session_state["resolve_blocked_id"] = bid
                    st.rerun()

            blk_target = st.session_state.get("resolve_blocked_id")
            if blk_target:
                blk_row = blk_by_id.get(int(blk_target))
                if blk_row is None:
                    st.session_state["resolve_blocked_id"] = None
                else:
                    with st.container(border=True):
                        st.subheader("Resolve blocked access")
                        mp_url = blk_row.get("media_plan_url", "") or ""
                        st.caption(
                            f"Campaign: {blk_row.get('campaign_name','')} | "
                            f"Region: {blk_row.get('region','')} | "
                            f"Error: {blk_row.get('error_message','')}"
                        )
                        if mp_url:
                            st.markdown(f"**Media plan:** [{mp_url}]({mp_url})")
                        st.info("Share the media plan with **anyone with the link** (Viewer), then re-run the Pipeline.")
                        blk_resolver = st.text_input("Your name", key=f"blk_by_{blk_target}")
                        blk_note = st.text_area("Resolution note (required)",
                                                key=f"blk_note_{blk_target}", height=100)
                        bc1, bc2 = st.columns([1, 1])
                        with bc1:
                            if st.button("Confirm resolve", type="primary",
                                         use_container_width=True, key=f"blk_confirm_{blk_target}"):
                                if not blk_resolver.strip():
                                    st.error("Please enter your name.")
                                elif not blk_note.strip():
                                    st.error("Please enter a resolution note.")
                                else:
                                    resolve_blocked(conn, int(blk_target),
                                                    resolved_by=blk_resolver, note=blk_note)
                                    st.session_state["resolve_blocked_id"] = None
                                    st.rerun()
                        with bc2:
                            if st.button("Cancel", use_container_width=True,
                                         key=f"blk_cancel_{blk_target}"):
                                st.session_state["resolve_blocked_id"] = None
                                st.rerun()

    conn.close()


if __name__ == "__main__":
    main()
