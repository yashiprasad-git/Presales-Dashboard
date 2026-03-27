"""
monitor_dashboard.py — DB Update Monitor

A separate, read-only dashboard that shows the status of daily DB updates
(db_updater.py runs). Completely independent of the analysis dashboard.
"""

import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import psycopg2
import psycopg2.extras
import streamlit as st

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Presales DB Monitor",
    page_icon="🗄️",
    layout="wide",
)

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _get_db_url() -> str:
    url = os.getenv("DATABASE_URL")
    if not url:
        try:
            url = st.secrets.get("DATABASE_URL", "")
        except Exception:
            pass
    return url or ""


@st.cache_resource(show_spinner=False)
def _schema_initialized() -> bool:
    """Run schema init once per server session."""
    try:
        from pipeline import get_db, init_schema
        conn = get_db()
        init_schema(conn)
        conn.close()
    except Exception:
        pass
    return True


def get_conn():
    _schema_initialized()
    return psycopg2.connect(_get_db_url())


def _df(conn, sql: str, params: tuple = ()) -> pd.DataFrame:
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        return pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()
    except Exception as e:
        st.error(f"DB query failed: {e}")
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Data fetchers
# ---------------------------------------------------------------------------

def fetch_pipeline_runs(conn) -> pd.DataFrame:
    return _df(conn, """
        SELECT run_id, started_at_utc, finished_at_utc, status
        FROM pipeline_runs
        ORDER BY started_at_utc DESC
        LIMIT 50
    """).fillna("")


def fetch_run_summary(conn) -> pd.DataFrame:
    """Per-run campaign stats derived from campaigns table."""
    return _df(conn, """
        SELECT
            run_id,
            COUNT(*)                                                      AS total_campaigns,
            SUM(CASE WHEN context_status LIKE '✅%' THEN 1 ELSE 0 END)   AS context_ok,
            SUM(CASE WHEN context_status LIKE '❌%' THEN 1 ELSE 0 END)   AS context_failed,
            SUM(CASE WHEN context_status IS NULL    THEN 1 ELSE 0 END)   AS context_pending
        FROM campaigns
        GROUP BY run_id
        ORDER BY MIN(inserted_at_utc) DESC
    """).fillna(0)


def fetch_campaign_summary(conn) -> pd.DataFrame:
    return _df(conn, """
        SELECT region,
               COUNT(*)                                                      AS total,
               SUM(CASE WHEN context_status LIKE '✅%' THEN 1 ELSE 0 END)   AS context_ok,
               SUM(CASE WHEN context_status LIKE '❌%' THEN 1 ELSE 0 END)   AS context_blocked,
               SUM(CASE WHEN context_status IS NULL    THEN 1 ELSE 0 END)   AS context_pending,
               SUM(CASE WHEN derived_language IS NOT NULL THEN 1 ELSE 0 END) AS analysed
        FROM campaigns
        GROUP BY region
        ORDER BY region
    """).fillna(0)


def fetch_blocked(conn) -> pd.DataFrame:
    return _df(conn, """
        SELECT region, campaign_name, brand, country,
               media_plan_url, error_message, date_flagged_utc, monday_url
        FROM access_blocked
        WHERE resolved_at_utc IS NULL
        ORDER BY date_flagged_utc DESC
    """).fillna("")


def fetch_context_status(conn) -> pd.DataFrame:
    return _df(conn, """
        SELECT region, campaign_name, brand_name, country,
               context_status, media_plan_url, monday_url, inserted_at_utc
        FROM campaigns
        ORDER BY inserted_at_utc DESC
    """).fillna("")


# ---------------------------------------------------------------------------
# Pipeline runner (manual trigger)
# ---------------------------------------------------------------------------

def run_db_updater(since_date: Optional[str] = None) -> str:
    monday_key = os.getenv("MONDAY_API_KEY") or st.secrets.get("MONDAY_API_KEY", "")
    db_url     = os.getenv("DATABASE_URL")    or st.secrets.get("DATABASE_URL", "")

    script = Path(__file__).parent / "db_updater.py"
    config = Path(__file__).parent / "monday_config.json"

    cmd = [sys.executable, str(script), str(config)]
    if since_date:
        cmd += ["--since", since_date]

    env = os.environ.copy()
    env["MONDAY_API_KEY"] = monday_key
    env["DATABASE_URL"]   = db_url

    result = subprocess.run(cmd, capture_output=True, text=True, env=env)
    return result.stdout + ("\n\nSTDERR:\n" + result.stderr if result.stderr else "")


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

def main():
    st.title("🗄️ Presales DB Monitor")
    st.caption("Monitors daily DB updates (Monday.com → PostgreSQL). "
               "Independent of the analysis dashboard.")

    conn = get_conn()

    # ── Sidebar — manual run ────────────────────────────────────────────
    with st.sidebar:
        st.header("▶ Run DB Update")
        st.caption("Manually trigger the DB updater (Steps 1 & 2 only — "
                   "no OpenAI, no inventory check).")
        since_input = st.text_input("Since date (YYYY-MM-DD)", placeholder="leave blank for auto")
        if st.button("▶ Run DB Update Now", use_container_width=True, type="primary"):
            since = since_input.strip() or None
            with st.spinner("Running DB update…"):
                output = run_db_updater(since)
            st.success("Run complete")
            st.text_area("Output", output, height=300)
            st.rerun()

        st.divider()
        st.caption("Analysis dashboard → [Open](https://silverpush-presales-dashboard.streamlit.app)")

    # ── Tabs ────────────────────────────────────────────────────────────
    tab_runs, tab_summary, tab_ctx, tab_blocked = st.tabs([
        "📋 Run History",
        "📊 Campaign Summary",
        "🔍 Context Status",
        "🚫 Access Blocked",
    ])

    # ── Run History ─────────────────────────────────────────────────────
    with tab_runs:
        st.subheader("Recent Pipeline Runs")
        df_runs    = fetch_pipeline_runs(conn)
        df_summary = fetch_run_summary(conn)

        if df_runs.empty:
            st.info("No pipeline runs found. Click **▶ Run DB Update Now** to start.")
        else:
            merged = df_runs.merge(df_summary, on="run_id", how="left").fillna(0)

            def _status_icon(s):
                return "✅" if s == "success" else ("⏳" if s == "running" else "❌")

            merged[""] = merged["status"].apply(_status_icon)
            merged = merged[["", "run_id", "started_at_utc", "finished_at_utc",
                              "status", "total_campaigns", "context_ok",
                              "context_failed", "context_pending"]]
            merged.columns = ["", "Run ID", "Started (UTC)", "Finished (UTC)",
                               "Status", "Campaigns", "Context OK",
                               "Context Failed", "Context Pending"]
            st.dataframe(merged, use_container_width=True, hide_index=True)

    # ── Campaign Summary ─────────────────────────────────────────────────
    with tab_summary:
        st.subheader("Campaigns by Region")
        df_camp = fetch_campaign_summary(conn)
        if df_camp.empty:
            st.info("No campaigns in DB yet.")
        else:
            total_row = pd.DataFrame([{
                "region": "TOTAL",
                "total": df_camp["total"].sum(),
                "context_ok": df_camp["context_ok"].sum(),
                "context_blocked": df_camp["context_blocked"].sum(),
                "context_pending": df_camp["context_pending"].sum(),
                "analysed": df_camp["analysed"].sum(),
            }])
            display = pd.concat([df_camp, total_row], ignore_index=True)
            display.columns = ["Region", "Total", "Context ✅", "Context ❌",
                                "Context ⏳", "Analysed"]
            st.dataframe(display, use_container_width=True, hide_index=True)

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total Campaigns", int(df_camp["total"].sum()))
            c2.metric("Context Extracted", int(df_camp["context_ok"].sum()))
            c3.metric("Access Blocked", int(df_camp["context_blocked"].sum()))
            c4.metric("Analysed", int(df_camp["analysed"].sum()))

    # ── Context Status ───────────────────────────────────────────────────
    with tab_ctx:
        st.subheader("Context Extraction Status per Campaign")
        df_ctx = fetch_context_status(conn)
        if df_ctx.empty:
            st.info("No campaigns yet.")
        else:
            regions = ["All"] + sorted([r for r in df_ctx["region"].unique() if r])
            f1, f2 = st.columns([1, 3])
            reg = f1.selectbox("Region", regions, key="ctx_reg")
            srch = f2.text_input("Search campaign / brand", key="ctx_srch").strip().lower()

            filt = df_ctx.copy()
            if reg != "All":
                filt = filt[filt["region"] == reg]
            if srch:
                hay = filt[["campaign_name", "brand_name"]].fillna("").astype(str)\
                          .agg(" ".join, axis=1).str.lower()
                filt = filt[hay.str.contains(srch, na=False)]

            st.write(f"Showing **{len(filt)}** / {len(df_ctx)} campaigns")
            col_cfg = {}
            if "monday_url" in filt.columns:
                col_cfg["monday_url"] = st.column_config.LinkColumn(
                    "Monday Link", display_text="Open")
            if "media_plan_url" in filt.columns:
                col_cfg["media_plan_url"] = st.column_config.LinkColumn(
                    "Media Plan", display_text="Open")
            st.dataframe(filt, use_container_width=True, height=500,
                         column_config=col_cfg, hide_index=True)

    # ── Access Blocked ────────────────────────────────────────────────────
    with tab_blocked:
        st.subheader("Media Plans — Access Blocked")
        df_blk = fetch_blocked(conn)
        if df_blk.empty:
            st.success("No blocked media plans. All accessible.")
        else:
            st.warning(f"{len(df_blk)} media plan(s) could not be accessed.")
            col_cfg_blk = {}
            if "monday_url" in df_blk.columns:
                col_cfg_blk["monday_url"] = st.column_config.LinkColumn(
                    "Monday Link", display_text="Open")
            if "media_plan_url" in df_blk.columns:
                col_cfg_blk["media_plan_url"] = st.column_config.LinkColumn(
                    "Media Plan", display_text="Open")
            st.dataframe(df_blk, use_container_width=True, height=400,
                         column_config=col_cfg_blk, hide_index=True)
            st.caption("Fix: Ask the file owner to set **Share → Anyone with the link → Viewer** "
                       "in Google Drive, then re-run the DB updater.")

    conn.close()


if __name__ == "__main__":
    main()
