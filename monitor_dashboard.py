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
from db_updater import retry_blocked

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
    try:
        return _df(conn, """
            SELECT
                run_id,
                COUNT(*)                                                      AS total_campaigns,
                SUM(CASE WHEN context_status LIKE '✅%%' THEN 1 ELSE 0 END)  AS context_ok,
                SUM(CASE WHEN context_status LIKE '❌%%' THEN 1 ELSE 0 END)  AS context_failed,
                SUM(CASE WHEN context_status IS NULL     THEN 1 ELSE 0 END)  AS context_pending
            FROM campaigns
            GROUP BY run_id
            ORDER BY MIN(inserted_at_utc) DESC
        """).fillna(0)
    except Exception:
        return pd.DataFrame(columns=["run_id", "total_campaigns",
                                     "context_ok", "context_failed", "context_pending"])


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
    st.title("🗄️ MDB Update Dashboard")
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
        st.subheader("🔄 Retry Failed Campaigns")
        st.caption("Re-attempts media plan reading for all ❌ campaigns immediately — "
                   "no second button click needed.")
        if st.button("🔄 Retry Now", use_container_width=True):
            with st.spinner("Re-attempting blocked/failed media plans…"):
                try:
                    summary = retry_blocked(conn)
                    st.success("Done")
                    st.text_area("Result", summary, height=250)
                    st.rerun()
                except Exception as e:
                    st.error(f"Error: {e}")

        st.divider()
        st.caption("Analysis dashboard → [Open](https://silverpush-presales-dashboard.streamlit.app)")

    # ── Tabs ────────────────────────────────────────────────────────────
    tab_runs, tab_ctx = st.tabs([
        "📋 Run History",
        "🔍 Campaign-wise Status",
    ])

    # ── Run History ─────────────────────────────────────────────────────
    with tab_runs:
        st.subheader("Recent Pipeline Runs")
        df_runs    = fetch_pipeline_runs(conn)
        df_summary = fetch_run_summary(conn)

        if df_runs.empty:
            st.info("No pipeline runs found. Click **▶ Run DB Update Now** to start.")
        else:
            if not df_summary.empty:
                merged = df_runs.merge(df_summary, on="run_id", how="left").fillna(0)
            else:
                merged = df_runs.copy()
                for col in ["total_campaigns", "context_ok", "context_failed", "context_pending"]:
                    merged[col] = 0

            def _status_icon(s):
                return "✅" if s == "success" else ("⏳" if s == "running" else "❌")

            merged[""] = merged["status"].apply(_status_icon)
            display_cols = ["", "run_id", "started_at_utc", "finished_at_utc",
                            "status", "total_campaigns", "context_ok",
                            "context_failed", "context_pending"]
            merged = merged[[c for c in display_cols if c in merged.columns]]
            merged.columns = ["", "Run ID", "Started (UTC)", "Finished (UTC)",
                               "Status", "Campaigns", "Context OK",
                               "Context Failed", "Context Pending"][:len(merged.columns)]
            st.dataframe(merged, use_container_width=True, hide_index=True)

    # ── Campaign-wise Status ─────────────────────────────────────────────
    with tab_ctx:
        st.subheader("Campaign-wise Status")
        df_ctx = fetch_context_status(conn)
        if df_ctx.empty:
            st.info("No campaigns yet.")
        else:
            ctx_col = df_ctx["context_status"].fillna("")

            # Known failure patterns → short display labels shown in the filter.
            # Add new entries here whenever a new failure reason is introduced.
            KNOWN_FAILURE_LABELS = {
                "media plan link not set": "❌ Media plan missing",
                "access blocked":          "❌ Access blocked",
                "context list not in standard format": "❌ Context list not in standard format",
                "context tab found but":   "❌ Context list not in standard format",
                "context tab not found":   "❌ Context tab not found",
            }

            def _short_label(raw: str) -> str:
                """Return a short display name for a raw ❌ context_status value."""
                lower = raw.lower()
                for keyword, label in KNOWN_FAILURE_LABELS.items():
                    if keyword in lower:
                        return label
                # Fallback: strip the leading ❌ and trim
                return raw.lstrip("❌").strip()

            # Map unique ❌ values → short labels (deduped by label)
            seen_labels: dict[str, str] = {}   # label → raw value (first match wins)
            for raw in sorted(ctx_col[ctx_col.str.startswith("❌")].unique()):
                label = _short_label(raw)
                if label not in seen_labels:
                    seen_labels[label] = raw

            status_options = (
                ["All", "✅ Success", "❌ All Failed", "⏳ Not yet processed"]
                + list(seen_labels.keys())
            )

            f1, f2, f3 = st.columns([1, 1, 2])
            reg  = f1.selectbox("Region",
                                ["All"] + sorted([r for r in df_ctx["region"].unique() if r]),
                                key="ctx_reg")
            stat = f2.selectbox("Status", status_options, key="ctx_stat")
            srch = f3.text_input("Search campaign / brand", key="ctx_srch").strip().lower()

            filt = df_ctx.copy()
            if reg != "All":
                filt = filt[filt["region"] == reg]

            if stat == "✅ Success":
                filt = filt[ctx_col.reindex(filt.index).str.startswith("✅", na=False)]
            elif stat == "❌ All Failed":
                filt = filt[ctx_col.reindex(filt.index).str.startswith("❌", na=False)]
            elif stat == "⏳ Not yet processed":
                filt = filt[ctx_col.reindex(filt.index) == ""]
            elif stat in seen_labels:
                # Match all ❌ rows whose short label equals the selected option
                matching_raws = [r for r, lbl in
                                 {v: _short_label(v) for v in
                                  ctx_col[ctx_col.str.startswith("❌")].unique()}.items()
                                 if lbl == stat]
                filt = filt[ctx_col.reindex(filt.index).isin(matching_raws)]

            if srch:
                hay = filt[["campaign_name", "brand_name"]].fillna("").astype(str)\
                          .agg(" ".join, axis=1).str.lower()
                filt = filt[hay.str.contains(srch, na=False)]

            # Drop media_plan_url — Monday link is sufficient
            filt = filt[[c for c in filt.columns if c != "media_plan_url"]]

            st.write(f"Showing **{len(filt)}** / {len(df_ctx)} campaigns")

            if stat in ("❌ Access blocked", "❌ All Failed") and len(filt) > 0:
                st.caption("Fix blocked media plans: ask the file owner to set "
                           "**Share → Anyone with the link → Viewer** in Google Drive, "
                           "then re-run the DB updater.")

            col_cfg = {}
            if "monday_url" in filt.columns:
                col_cfg["monday_url"] = st.column_config.LinkColumn(
                    "Monday Link", display_text="Open")
            st.dataframe(filt, use_container_width=True, height=500,
                         column_config=col_cfg, hide_index=True)

    conn.close()


if __name__ == "__main__":
    main()
