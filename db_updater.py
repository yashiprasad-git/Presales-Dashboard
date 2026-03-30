"""
db_updater.py — Part 1: Monday.com → PostgreSQL (Steps 1 & 2 only)

Fetches Done campaigns from Monday.com, reads media plan context lists,
and writes everything to the database.
NO OpenAI. NO inventory check. Fully independent of the analysis pipeline.

Designed to run:
  - Automatically via GitHub Actions on a daily schedule
  - Manually: python3 db_updater.py monday_config.json
"""

import argparse
import datetime
import sys
from datetime import timezone

import psycopg2.extras

from pipeline import (
    FIRST_RUN_SINCE,
    MONDAY_ITEM_URL,
    _format_col_value,
    _get_env,
    _insert_campaign,
    _insert_context_rows,
    _log_run_finish,
    _log_run_start,
    _update_context_status,
    _upsert_blocked,
    _already_complete,
    fetch_board_items,
    fetch_item_media_url,
    find_context_tab,
    get_db,
    init_schema,
    load_config,
    read_context_rows,
    read_public_sheet,
    should_include,
    utc_now_iso,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _last_run_since(conn) -> str:
    """Return date of last successful run, or FIRST_RUN_SINCE on first run."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT started_at_utc FROM pipeline_runs
                WHERE status = 'success'
                ORDER BY started_at_utc DESC LIMIT 1
            """)
            row = cur.fetchone()
            if row:
                return row[0][:10]
    except Exception:
        pass
    return FIRST_RUN_SINCE


# ---------------------------------------------------------------------------
# Retry blocked — re-attempt Step 2 for previously blocked campaigns
# ---------------------------------------------------------------------------

def retry_blocked(conn, monday_key: str = "") -> str:
    """
    Re-attempt Step 2 for ALL ❌ campaigns (including 'media plan missing').
    - If media_plan_url is NULL in DB, re-fetches it from Monday.com first.
    - Clears old status, then immediately re-runs context extraction.
    Returns a human-readable summary string.
    """
    import psycopg2.extras

    if not monday_key:
        monday_key = _get_env("MONDAY_API_KEY") or ""

    run_id = datetime.datetime.now(timezone.utc).strftime("RETRY_%Y%m%d_%H%M%S")
    lines  = []

    # Include ALL ❌ campaigns — even "media plan missing" ones
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT monday_item_id, monday_board_id, monday_url, region,
                   campaign_name, brand_name, country, vertical,
                   rfp_summary, targeting, any_other_details, media_plan_url
            FROM campaigns
            WHERE context_status ILIKE '❌%'
              AND NOT EXISTS (
                  SELECT 1 FROM context_rows cr
                  WHERE cr.monday_item_id = campaigns.monday_item_id
              )
        """)
        to_retry = [dict(r) for r in cur.fetchall()]

    if not to_retry:
        return "No blocked/failed campaigns to retry."

    lines.append(f"Retrying {len(to_retry)} campaign(s)…\n")

    # Re-fetch media_plan_url from Monday for campaigns where it's NULL
    if monday_key:
        for camp in to_retry:
            if not camp.get("media_plan_url"):
                item_id = camp["monday_item_id"]
                try:
                    url = fetch_item_media_url(monday_key, item_id)
                    if url:
                        camp["media_plan_url"] = url
                        with conn.cursor() as cur:
                            cur.execute(
                                "UPDATE campaigns SET media_plan_url = %s WHERE monday_item_id = %s",
                                (url, item_id),
                            )
                        conn.commit()
                        lines.append(f"  🔗 {camp.get('campaign_name')} — fetched URL from Monday")
                except Exception as e:
                    lines.append(f"  ⚠️  {camp.get('campaign_name')} — could not fetch URL: {e}")
    else:
        lines.append("  ⚠️  No MONDAY_API_KEY — skipping URL re-fetch for missing links\n")

    # Clear their status so fresh results can be written
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE campaigns SET context_status = NULL
            WHERE context_status ILIKE '❌%'
              AND NOT EXISTS (
                  SELECT 1 FROM context_rows cr
                  WHERE cr.monday_item_id = campaigns.monday_item_id
              )
        """)
    conn.commit()

    ctx_ok = ctx_fail = 0
    for camp in to_retry:
        item_id        = camp["monday_item_id"]
        board_id       = camp.get("monday_board_id", "")
        media_plan_url = camp.get("media_plan_url") or ""
        name           = camp.get("campaign_name", item_id)

        if not media_plan_url:
            _update_context_status(conn, item_id, "❌ Media plan link not set on Monday.com")
            lines.append(f"  ❌ {name} — no media plan URL")
            ctx_fail += 1
            continue

        meta = {**camp, "brief": " | ".join(filter(None, [
            camp.get("rfp_summary"), camp.get("targeting"),
            camp.get("any_other_details"),
        ])), "media_plan_url": media_plan_url}

        try:
            xls = read_public_sheet(media_plan_url)
        except PermissionError as e:
            reason = str(e)
            _upsert_blocked(conn, run_id, item_id, int(board_id or 0), meta, reason)
            _update_context_status(conn, item_id,
                "❌ Access blocked – fix: Share → Anyone with the link → Viewer")
            lines.append(f"  ❌ {name} — still access blocked")
            ctx_fail += 1
            continue
        except Exception as e:
            reason = str(e)
            _upsert_blocked(conn, run_id, item_id, int(board_id or 0), meta, reason)
            _update_context_status(conn, item_id, f"❌ Could not read media plan – {reason}")
            lines.append(f"  ❌ {name} — {reason}")
            ctx_fail += 1
            continue

        tab = find_context_tab(xls)
        if not tab:
            _upsert_blocked(conn, run_id, item_id, int(board_id or 0), meta, "No 'context' tab found")
            _update_context_status(conn, item_id, "❌ No 'Context' tab found in media plan")
            lines.append(f"  ❌ {name} — no Context tab")
            ctx_fail += 1
            continue

        try:
            ctx_rows, local_lang = read_context_rows(xls, tab)
        except Exception as e:
            reason = f"Context parse error: {e}"
            _upsert_blocked(conn, run_id, item_id, int(board_id or 0), meta, reason)
            _update_context_status(conn, item_id, f"❌ {reason}")
            lines.append(f"  ❌ {name} — {reason}")
            ctx_fail += 1
            continue

        if ctx_rows:
            _insert_context_rows(conn, run_id, item_id, int(board_id or 0),
                                  meta, ctx_rows, local_lang)
            lang_label = local_lang or "English"
            _update_context_status(conn, item_id, f"✅ {len(ctx_rows)} rows saved ({lang_label})")
            lines.append(f"  ✅ {name} — {len(ctx_rows)} rows saved ({lang_label})")
            ctx_ok += 1
        else:
            _upsert_blocked(conn, run_id, item_id, int(board_id or 0), meta,
                            "Context list not in standard format")
            _update_context_status(conn, item_id, "❌ Context list not in standard format")
            lines.append(f"  ❌ {name} — context list not in standard format")
            ctx_fail += 1

    lines.append(f"\nDone — ✅ {ctx_ok} recovered  |  ❌ {ctx_fail} still failed")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="DB Updater — fetch Monday.com campaigns + context lists → PostgreSQL"
    )
    parser.add_argument("config", help="Path to monday_config.json")
    parser.add_argument("--since", default=None,
                        help="Process campaigns created on/after YYYY-MM-DD "
                             f"(default: last successful run or {FIRST_RUN_SINCE})")
    parser.add_argument("--retry-blocked", action="store_true",
                        help="Re-attempt media plan reading for all blocked/failed campaigns")
    args = parser.parse_args()

    monday_key = _get_env("MONDAY_API_KEY")
    if not monday_key:
        raise SystemExit("❌ MONDAY_API_KEY is not set.")

    conn = get_db()
    init_schema(conn)

    # Retry-blocked mode — re-attempt Step 2 only, then exit
    if args.retry_blocked:
        print("\nRetry-blocked mode: re-attempting all ❌ campaigns…")
        summary = retry_blocked(conn)
        print(summary)
        conn.close()
        return

    run_id     = datetime.datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    since_str  = args.since or _last_run_since(conn)
    since_dt   = datetime.datetime.strptime(since_str, "%Y-%m-%d")
    started_at = utc_now_iso()

    _log_run_start(conn, run_id, started_at)

    print(f"\n{'='*60}")
    print(f"DB Updater run: {run_id}  |  since: {since_str}")
    print(f"{'='*60}")

    boards = load_config(args.config)

    # ────────────────────────────────────────────────────────────────────
    # STEP 1 — Monday.com → campaigns table
    # ────────────────────────────────────────────────────────────────────
    print("\n── STEP 1: Fetching campaigns from Monday.com")

    new_campaigns = 0
    for board in boards:
        region   = board["region"]
        board_id = board["board_id"]
        print(f"\n   Board: {region}")

        try:
            items = fetch_board_items(monday_key, board_id)
        except Exception as e:
            print(f"   ERROR fetching board: {e}")
            continue

        saved = skipped_date = skipped_done = skipped_product = 0
        for item in items:
            group = (item.get("group") or {}).get("title", "")
            if not group.lower().startswith("done"):
                continue

            created_str = item.get("created_at") or ""
            try:
                created_dt = datetime.datetime.fromisoformat(
                    created_str.replace("Z", "+00:00")).replace(tzinfo=None)
                if created_dt < since_dt:
                    skipped_date += 1
                    continue
            except Exception:
                pass

            item_id = str(item["id"])

            if _already_complete(conn, item_id):
                skipped_done += 1
                continue

            col_values = {cv["id"]: _format_col_value(cv)
                          for cv in item.get("column_values", [])}

            if not should_include(col_values, board):
                skipped_product += 1
                continue

            monday_url    = MONDAY_ITEM_URL.format(board_id=board_id, item_id=item_id)
            product_parts = [str(col_values.get(cid, "") or "")
                             for cid in board["product_col_ids"]]

            if "APAC" in region.upper():
                platform_val = str(col_values.get(
                    board.get("platform_col_id", ""), "") or "").strip()
                if platform_val and platform_val not in product_parts:
                    product_parts.append(platform_val)

            data = {
                "monday_url":          monday_url,
                "campaign_name":       item.get("name", ""),
                "brand_name":          col_values.get(board["col_brand"], ""),
                "vertical":            col_values.get(board["col_vertical"], ""),
                "country":             col_values.get(board["col_country"], ""),
                "run_dates":           col_values.get(board["col_run_dates"], ""),
                "rfp_summary":         col_values.get(board["col_rfp"], ""),
                "targeting":           col_values.get(board["col_targeting"], ""),
                "trigger_list":        col_values.get(board["col_trigger"], ""),
                "any_other_details":   col_values.get(board["col_other"], ""),
                "products_to_pitch":   " | ".join(filter(None, product_parts)),
                "monday_submitted_at": created_str,
                "media_plan_url":      col_values.get(board["col_media_plan"], ""),
            }

            _insert_campaign(conn, run_id, item_id, board_id, region, data)
            saved += 1
            new_campaigns += 1

        print(f"   Saved: {saved} | Skipped (date): {skipped_date} | "
              f"Skipped (done): {skipped_done} | Skipped (product): {skipped_product}")

    print(f"\n   ✓ Step 1 complete — {new_campaigns} new campaign(s) saved to DB")

    # ────────────────────────────────────────────────────────────────────
    # STEP 2 — Google Sheets → context_rows + access_blocked
    # media_plan_url is already stored in campaigns table from Step 1
    # ────────────────────────────────────────────────────────────────────
    print("\n── STEP 2: Reading media plans (Google Sheets)")

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT monday_item_id, monday_board_id, monday_url, region,
                   campaign_name, brand_name, country, vertical,
                   rfp_summary, targeting, any_other_details, media_plan_url
            FROM campaigns
            WHERE NOT EXISTS (
                SELECT 1 FROM context_rows cr
                WHERE cr.monday_item_id = campaigns.monday_item_id
            )
            AND context_status IS NULL
        """)
        campaigns_needing_context = [dict(r) for r in cur.fetchall()]

    ctx_processed = ctx_blocked = ctx_no_link = 0
    for camp in campaigns_needing_context:
        item_id        = camp["monday_item_id"]
        board_id       = camp.get("monday_board_id", "")
        media_plan_url = camp.get("media_plan_url") or ""

        if not media_plan_url:
            _update_context_status(conn, item_id,
                                   "❌ Media plan link not set on Monday.com")
            ctx_no_link += 1
            continue

        meta = {**camp, "brief": " | ".join(filter(None, [
            camp.get("rfp_summary"), camp.get("targeting"),
            camp.get("any_other_details"),
        ])), "media_plan_url": media_plan_url}

        print(f"   Reading: {camp.get('campaign_name')} ({camp.get('region')})")
        try:
            xls = read_public_sheet(media_plan_url)
        except PermissionError as e:
            reason = str(e)
            print(f"   BLOCKED: {reason}")
            _upsert_blocked(conn, run_id, item_id, int(board_id or 0), meta, reason)
            _update_context_status(conn, item_id,
                "❌ Access blocked – fix: Share → Anyone with the link → Viewer")
            ctx_blocked += 1
            continue
        except Exception as e:
            reason = str(e)
            print(f"   FAILED: {reason}")
            _upsert_blocked(conn, run_id, item_id, int(board_id or 0), meta, reason)
            _update_context_status(conn, item_id,
                f"❌ Could not read media plan – {reason}")
            ctx_blocked += 1
            continue

        tab = find_context_tab(xls)
        if not tab:
            _upsert_blocked(conn, run_id, item_id, int(board_id or 0), meta,
                            "No 'context' tab found")
            _update_context_status(conn, item_id,
                "❌ No 'Context' tab found in media plan")
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
            _insert_context_rows(conn, run_id, item_id, int(board_id or 0),
                                  meta, ctx_rows, local_lang)
            lang_label = local_lang or "English"
            print(f"   ✓ {len(ctx_rows)} context rows saved | lang: {lang_label}")
            _update_context_status(conn, item_id,
                f"✅ {len(ctx_rows)} rows saved ({lang_label})")
            ctx_processed += 1
        else:
            _upsert_blocked(conn, run_id, item_id, int(board_id or 0), meta,
                            "Context list not in standard format")
            _update_context_status(conn, item_id,
                "❌ Context list not in standard format")
            ctx_blocked += 1

    print(f"\n   ✓ Step 2 complete — Processed: {ctx_processed} | "
          f"Blocked: {ctx_blocked} | No link: {ctx_no_link}")

    _log_run_finish(conn, run_id, "success")
    conn.close()

    print(f"\n{'='*60}")
    print(f"✅ DB Update complete — Run: {run_id}")
    print(f"   New campaigns : {new_campaigns}")
    print(f"   Context saved : {ctx_processed}")
    print(f"   Access blocked: {ctx_blocked}")
    print(f"   No link       : {ctx_no_link}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"\n❌ DB Updater failed: {exc}", file=sys.stderr)
        sys.exit(1)
