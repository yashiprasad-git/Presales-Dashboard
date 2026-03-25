import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import streamlit as st

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    psycopg2 = None  # type: ignore


APP_TITLE     = "Presales Inventory Dashboard"
PRESALES_DIR  = Path(__file__).resolve().parent
PIPELINE_SCRIPT = PRESALES_DIR / "pipeline.py"
FIRST_RUN_SINCE = "2026-03-20"


# ---------------------------------------------------------------------------
# DB connection
# ---------------------------------------------------------------------------

def _get_db_url() -> str:
    url = os.getenv("DATABASE_URL") or st.secrets.get("DATABASE_URL", "")  # type: ignore[attr-defined]
    if not url:
        raise RuntimeError("DATABASE_URL is not set.")
    return url


def get_db():
    if psycopg2 is None:
        raise RuntimeError("psycopg2 not installed. Run: pip install psycopg2-binary")
    conn = psycopg2.connect(_get_db_url())
    conn.autocommit = False
    return conn


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@st.cache_resource(show_spinner=False)
def _ensure_schema() -> bool:
    """Run schema init ONCE per server session."""
    from pipeline import init_schema
    conn = get_db()
    try:
        init_schema(conn)
    finally:
        conn.close()
    return True


def get_dashboard_conn():
    _ensure_schema()
    return get_db()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _df(conn, sql: str, params: tuple = ()) -> pd.DataFrame:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params or None)
        rows = cur.fetchall()
    return pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()


def _last_run_date(conn) -> Optional[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT started_at_utc FROM pipeline_runs WHERE status='success' "
            "ORDER BY started_at_utc DESC LIMIT 1"
        )
        row = cur.fetchone()
    return str(row[0])[:10] if row and row[0] else None


def _last_updated(conn) -> Optional[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT MAX(inserted_at_utc) FROM campaigns"
        )
        row = cur.fetchone()
    return str(row[0])[:19] if row and row[0] else None


def resolve_alert(conn, alert_id: int, resolved_by: str, note: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE alerts SET resolved_at_utc=%s, resolved_by=%s, resolved_note=%s "
            "WHERE alert_id=%s",
            (utc_now_iso(), resolved_by.strip() or None, note.strip() or None, alert_id),
        )
    conn.commit()


def resolve_blocked(conn, blocked_id: int, resolved_by: str, note: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE access_blocked SET resolved_at_utc=%s, resolved_by=%s, resolved_note=%s "
            "WHERE id=%s",
            (utc_now_iso(), resolved_by.strip() or None, note.strip() or None, blocked_id),
        )
    conn.commit()


# ---------------------------------------------------------------------------
# Data fetchers
# ---------------------------------------------------------------------------

def fetch_open_alerts(conn) -> pd.DataFrame:
    return _df(conn, """
        SELECT alert_id, region, campaign_name, brand_name, country,
               derived_language, products_to_pitch, monday_run_dates,
               recommended_category, inventory_status,
               p1_channel_count, p2_channel_count, p3_channel_count,
               available_inventory_count, error_log, monday_url, date_flagged_utc
        FROM alerts
        WHERE resolved_at_utc IS NULL
        ORDER BY date_flagged_utc DESC
    """).fillna("")


def fetch_resolved_alerts(conn) -> pd.DataFrame:
    return _df(conn, """
        SELECT alert_id, region, campaign_name, brand_name, country,
               derived_language, recommended_category, inventory_status,
               available_inventory_count, monday_url, date_flagged_utc,
               resolved_at_utc, resolved_by, resolved_note
        FROM alerts
        WHERE resolved_at_utc IS NOT NULL
        ORDER BY resolved_at_utc DESC
    """).fillna("")


def fetch_campaigns(conn, days: Optional[int] = 30, older: bool = False) -> pd.DataFrame:
    if days is not None:
        from datetime import timedelta
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat(timespec="seconds")
        clause = "WHERE inserted_at_utc < %s" if older else "WHERE inserted_at_utc >= %s"
        params: tuple = (cutoff,)
    else:
        clause, params = "", ()

    return _df(conn, f"""
        SELECT region, campaign_name, brand_name, vertical, country, run_dates,
               derived_language, recommended_category,
               inventory_status, available_inventory_count,
               p1_channel_count, p2_channel_count, p3_channel_count,
               error_log, monday_url, inserted_at_utc
        FROM campaigns
        {clause}
        ORDER BY inserted_at_utc DESC
    """, params).fillna("")


def fetch_access_blocked(conn) -> pd.DataFrame:
    return _df(conn, """
        SELECT id, region, campaign_name, brand, country,
               monday_url, media_plan_url, error_message,
               date_flagged_utc
        FROM access_blocked
        WHERE resolved_at_utc IS NULL
        ORDER BY date_flagged_utc DESC
    """).fillna("")


# ---------------------------------------------------------------------------
# Pipeline runner
# ---------------------------------------------------------------------------

def run_pipeline(config_path: Path, inventory_path: Path, since_date: str) -> str:
    monday_key = os.getenv("MONDAY_API_KEY") or st.secrets.get("MONDAY_API_KEY", "")  # type: ignore[attr-defined]
    openai_key = os.getenv("OPENAI_API_KEY") or st.secrets.get("OPENAI_API_KEY", "")  # type: ignore[attr-defined]
    db_url     = os.getenv("DATABASE_URL")    or st.secrets.get("DATABASE_URL", "")    # type: ignore[attr-defined]
    if not monday_key:
        raise RuntimeError("MONDAY_API_KEY is not set.")
    if not openai_key:
        raise RuntimeError("OPENAI_API_KEY is not set.")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set.")

    env = os.environ.copy()
    env["MONDAY_API_KEY"] = monday_key
    env["OPENAI_API_KEY"] = openai_key
    env["DATABASE_URL"]   = db_url

    proc = subprocess.run(
        [sys.executable, str(PIPELINE_SCRIPT),
         str(config_path), str(inventory_path), "--since", since_date],
        cwd=str(PRESALES_DIR), capture_output=True, text=True, env=env,
    )
    output = (proc.stdout or "") + ("\n" + proc.stderr if proc.stderr else "")
    if proc.returncode != 0:
        raise RuntimeError(output or "Pipeline failed.")
    return output


# ---------------------------------------------------------------------------
# Sidebar config
# ---------------------------------------------------------------------------

def _find_file(glob: str) -> Optional[Path]:
    matches = list(PRESALES_DIR.glob(glob))
    return max(matches, key=lambda p: p.stat().st_mtime) if matches else None


def _sidebar(conn) -> tuple:
    with st.sidebar:
        st.header("Configuration")

        config_path    = st.text_input("Config file",    value="monday_config.json")
        inventory_path = st.text_input("Inventory file", value="inventory.xlsx")

        st.divider()
        st.caption("API Keys")
        mk = os.getenv("MONDAY_API_KEY") or st.secrets.get("MONDAY_API_KEY")  # type: ignore[attr-defined]
        ok = os.getenv("OPENAI_API_KEY") or st.secrets.get("OPENAI_API_KEY")  # type: ignore[attr-defined]
        dk = os.getenv("DATABASE_URL")   or st.secrets.get("DATABASE_URL")    # type: ignore[attr-defined]
        st.write("`MONDAY_API_KEY`:",  "✅ set" if mk else "❌ missing")
        st.write("`OPENAI_API_KEY`:",  "✅ set" if ok else "❌ missing")
        st.write("`DATABASE_URL`:",    "✅ set" if dk else "❌ missing")

        st.divider()
        since_preview = _last_run_date(conn) or FIRST_RUN_SINCE
        st.caption(f"Will pick up campaigns created on/after: **{since_preview}**")
        run_clicked = st.button("▶ Run Pipeline", type="primary", use_container_width=True)

    return config_path, inventory_path, run_clicked


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)

    conn = get_dashboard_conn()

    updated = _last_updated(conn)
    st.caption(f"Last updated: **{updated or 'Never'}** (UTC)")

    if "resolve_alert_id"  not in st.session_state: st.session_state["resolve_alert_id"]  = None
    if "resolve_blocked_id" not in st.session_state: st.session_state["resolve_blocked_id"] = None

    config_path, inventory_path, run_clicked = _sidebar(conn)

    if run_clicked:
        since = _last_run_date(conn) or FIRST_RUN_SINCE
        conn.close()   # close before long subprocess
        try:
            with st.spinner(
                f"Running pipeline… picking up 'Done' campaigns created on/after **{since}**. "
                "This takes a few minutes."
            ):
                output = run_pipeline(Path(config_path), Path(inventory_path), since)
            st.success("Pipeline completed successfully.")
            with st.expander("Pipeline output"):
                st.text(output)
        except Exception as e:
            st.error(str(e))
        finally:
            conn = get_dashboard_conn()
        st.rerun()

    # ── Tabs ──────────────────────────────────────────────────────────────
    tab_alerts, tab_resolved, tab_30d, tab_old, tab_blocked = st.tabs([
        "Open Alerts", "Resolved Alerts",
        "All Results (Last 30 days)", "Older Campaigns",
        "Access Blocked",
    ])

    # ── Open Alerts ──────────────────────────────────────────────────────
    with tab_alerts:
        df_a = fetch_open_alerts(conn)
        regions  = ["All"] + sorted([r for r in df_a["region"].unique() if r]) if not df_a.empty else ["All"]
        statuses = ["All"] + sorted([s for s in df_a["inventory_status"].unique() if s]) if not df_a.empty else ["All"]

        c1, c2 = st.columns(2)
        reg_f = c1.selectbox("Filter by Region", regions, key="al_reg")
        sta_f = c2.selectbox("Filter by Status", statuses, key="al_sta")

        filt = df_a.copy()
        if not filt.empty:
            if reg_f != "All": filt = filt[filt["region"] == reg_f]
            if sta_f != "All": filt = filt[filt["inventory_status"] == sta_f]

        st.write(f"Open alerts: **{len(filt)}**")
        if filt.empty:
            st.info("No open alerts.")
        else:
            widths = [1.8, 1.8, 1.8, 1.8, 2.5, 2.8, 1.5, 0.9, 1.6]
            hdr = st.columns(widths)
            for i, h in enumerate(["Region", "Brand", "Country", "Language",
                                    "Run Dates", "Category", "Status", "Link", "Resolve"]):
                hdr[i].markdown(f"**{h}**")

            row_by_id = {int(r["alert_id"]): r for _, r in filt.iterrows()}
            for _, row in filt.iterrows():
                aid  = int(row["alert_id"])
                cols = st.columns(widths)
                cols[0].write(row.get("region", ""))
                cols[1].write(row.get("brand_name", ""))
                cols[2].write(row.get("country", ""))
                cols[3].write(row.get("derived_language", ""))
                cols[4].write(row.get("monday_run_dates", "") or "")
                cols[5].write(row.get("recommended_category", ""))
                cols[6].write(row.get("inventory_status", ""))
                url = row.get("monday_url", "") or ""
                cols[7].markdown(f"[Open]({url})" if url else "—")
                if cols[8].button("Resolve", key=f"al_res_{aid}", use_container_width=True):
                    st.session_state["resolve_alert_id"] = aid
                    st.rerun()

            target_id = st.session_state.get("resolve_alert_id")
            if target_id:
                trow = row_by_id.get(int(target_id))
                if trow is None:
                    st.session_state["resolve_alert_id"] = None
                else:
                    with st.container(border=True):
                        st.subheader("Resolve alert")
                        st.caption(
                            f"Campaign: {trow.get('campaign_name','')} | "
                            f"Brand: {trow.get('brand_name','')} | "
                            f"Status: {trow.get('inventory_status','')}"
                        )
                        resolver = st.text_input("Your name", key=f"al_by_{target_id}")
                        default_note = (
                            f"Available: {trow.get('available_inventory_count','')}\n"
                            f"P1: {trow.get('p1_channel_count','')}  "
                            f"P2: {trow.get('p2_channel_count','')}  "
                            f"P3: {trow.get('p3_channel_count','')}"
                        )
                        note = st.text_area("Resolution note (required)",
                                            value=default_note, key=f"al_note_{target_id}", height=120)
                        ok_col, cancel_col = st.columns(2)
                        with ok_col:
                            if st.button("Confirm", type="primary", use_container_width=True,
                                         key=f"al_ok_{target_id}"):
                                if not resolver.strip():
                                    st.error("Please enter your name.")
                                elif not note.strip():
                                    st.error("Please enter a note.")
                                else:
                                    resolve_alert(conn, target_id, resolver, note)
                                    st.session_state["resolve_alert_id"] = None
                                    st.rerun()
                        with cancel_col:
                            if st.button("Cancel", use_container_width=True, key=f"al_can_{target_id}"):
                                st.session_state["resolve_alert_id"] = None
                                st.rerun()

    # ── Resolved Alerts ───────────────────────────────────────────────────
    with tab_resolved:
        df_r = fetch_resolved_alerts(conn)
        if df_r.empty:
            st.info("No resolved alerts yet.")
        else:
            st.write(f"Resolved alerts: **{len(df_r)}**")
            col_cfg = {}
            if "monday_url" in df_r.columns:
                col_cfg["monday_url"] = st.column_config.LinkColumn("Monday Link", display_text="Open")
            st.dataframe(df_r, use_container_width=True, height=600, column_config=col_cfg)

    # ── All Results – last 30 days ────────────────────────────────────────
    with tab_30d:
        df_30 = fetch_campaigns(conn, days=30, older=False)
        if df_30.empty:
            st.info("No results in the last 30 days. Click **▶ Run Pipeline** to generate results.")
        else:
            regions_30 = ["All"] + sorted([r for r in df_30["region"].unique() if r])
            status_30  = ["All"] + sorted([s for s in df_30["inventory_status"].unique() if s])
            f1, f2, f3 = st.columns([1, 1, 2])
            reg_30 = f1.selectbox("Region", regions_30, key="r30_reg")
            sta_30 = f2.selectbox("Status", status_30,  key="r30_sta")
            srch   = f3.text_input("Search (brand / campaign / category / country)",
                                   key="r30_srch").strip().lower()

            filt30 = df_30.copy()
            if reg_30 != "All": filt30 = filt30[filt30["region"] == reg_30]
            if sta_30 != "All": filt30 = filt30[filt30["inventory_status"] == sta_30]
            if srch:
                hay = filt30[["campaign_name", "brand_name", "country",
                              "recommended_category"]].fillna("").astype(str).agg(" ".join, axis=1).str.lower()
                filt30 = filt30[hay.str.contains(srch, na=False)]

            st.write(f"Rows shown: **{len(filt30)}** / Total: **{len(df_30)}**")
            col_cfg30 = {}
            if "monday_url" in filt30.columns:
                col_cfg30["monday_url"] = st.column_config.LinkColumn("Monday Link", display_text="Open")
            st.dataframe(filt30, use_container_width=True, height=600, column_config=col_cfg30)

    # ── Older Campaigns ───────────────────────────────────────────────────
    with tab_old:
        df_old = fetch_campaigns(conn, days=30, older=True)
        if df_old.empty:
            st.info("No campaigns older than 30 days.")
        else:
            st.write(f"Older campaigns: **{len(df_old)}**")
            old_cfg = {}
            if "monday_url" in df_old.columns:
                old_cfg["monday_url"] = st.column_config.LinkColumn("Monday Link", display_text="Open")
            st.dataframe(df_old, use_container_width=True, height=600, column_config=old_cfg)

    # ── Access Blocked ────────────────────────────────────────────────────
    with tab_blocked:
        df_blk = fetch_access_blocked(conn)
        st.write(f"Access blocked (open): **{len(df_blk)}**")

        if df_blk.empty:
            st.info("No access-blocked media plans.")
        else:
            blk_widths = [2, 2, 2, 3, 2, 1.5]
            hdr = st.columns(blk_widths)
            for i, h in enumerate(["Region", "Campaign", "Brand", "Error", "Flagged", "Resolve"]):
                hdr[i].markdown(f"**{h}**")

            blk_by_id = {int(r["id"]): r for _, r in df_blk.iterrows()}

            for _, row in df_blk.iterrows():
                bid  = int(row["id"])
                cols = st.columns(blk_widths)
                cols[0].write(row.get("region", ""))
                url  = row.get("monday_url", "") or ""
                name = row.get("campaign_name", "") or ""
                cols[1].markdown(f"[{name}]({url})" if url else name)
                cols[2].write(row.get("brand", ""))
                cols[3].write(str(row.get("error_message", ""))[:120])
                cols[4].write(str(row.get("date_flagged_utc", ""))[:10])
                if cols[5].button("Resolve", key=f"blk_res_{bid}", use_container_width=True):
                    st.session_state["resolve_blocked_id"] = bid
                    st.rerun()

            blk_target = st.session_state.get("resolve_blocked_id")
            if blk_target:
                brow = blk_by_id.get(int(blk_target))
                if brow is None:
                    st.session_state["resolve_blocked_id"] = None
                else:
                    with st.container(border=True):
                        st.subheader("Resolve blocked access")
                        mp = brow.get("media_plan_url", "") or ""
                        st.caption(
                            f"Campaign: {brow.get('campaign_name','')} | "
                            f"Region: {brow.get('region','')} | "
                            f"Error: {brow.get('error_message','')}"
                        )
                        if mp:
                            st.markdown(f"**Media plan:** [{mp}]({mp})")
                        st.info("Share the media plan with **Anyone with the link → Viewer**, then re-run the pipeline.")
                        blk_by = st.text_input("Your name", key=f"blk_by_{blk_target}")
                        blk_note = st.text_area("Resolution note (required)",
                                                key=f"blk_note_{blk_target}", height=100)
                        bc1, bc2 = st.columns(2)
                        with bc1:
                            if st.button("Confirm", type="primary", use_container_width=True,
                                         key=f"blk_ok_{blk_target}"):
                                if not blk_by.strip():
                                    st.error("Please enter your name.")
                                elif not blk_note.strip():
                                    st.error("Please enter a note.")
                                else:
                                    resolve_blocked(conn, int(blk_target), blk_by, blk_note)
                                    st.session_state["resolve_blocked_id"] = None
                                    st.rerun()
                        with bc2:
                            if st.button("Cancel", use_container_width=True,
                                         key=f"blk_can_{blk_target}"):
                                st.session_state["resolve_blocked_id"] = None
                                st.rerun()

    conn.close()


if __name__ == "__main__":
    main()
