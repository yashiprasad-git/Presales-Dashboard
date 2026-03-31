# Presales / MDB — PostgreSQL database schema (for IT)

This document describes the database objects the application expects. The application uses **PostgreSQL** (12+ recommended; 14+ preferred) and connects via `psycopg2` using a standard connection URI.

**Charset:** UTF-8 (default for PostgreSQL `TEXT`).

**Note:** The application creates these tables automatically on first run (`init_schema`). IT may either provision an empty database and let the app create objects, or create the schema below manually for review / hardening.

---

## Summary

| Table             | Purpose |
|-------------------|---------|
| `pipeline_runs`   | Log of each `db_updater.py` run (start/end time, status, optional logs). |
| `campaigns`       | One row per Monday.com campaign ingested; Monday fields + analysis fields + context status. |
| `context_rows`    | Extracted tactic / sub-tactic / signal rows from media plans (multilingual). |
| `access_blocked`  | Media plans that could not be read (access or parse issues); optional resolution fields. |
| `alerts`          | Inventory / category alerts from the analysis dashboard (optional if only MDB updater is deployed). |

---

## 1. `pipeline_runs`

Tracks each execution of the DB updater pipeline.

| Column            | Type    | Nullable | Description |
|-------------------|---------|----------|-------------|
| `run_id`          | TEXT    | NOT NULL | Primary key; unique run identifier (e.g. timestamp-based string). |
| `started_at_utc`  | TEXT    | NOT NULL | ISO-8601 timestamp (UTC). |
| `finished_at_utc` | TEXT    | YES      | ISO-8601 timestamp when run finished. |
| `status`          | TEXT    | NOT NULL | e.g. `success`, `running`, `failed`. |
| `stdout`          | TEXT    | YES      | Optional captured standard output. |
| `stderr`          | TEXT    | YES      | Optional captured standard error. |

**Primary key:** `run_id`

---

## 2. `campaigns`

Core campaign record from Monday.com plus optional analysis and context-extraction fields.

| Column                     | Type     | Nullable | Description |
|----------------------------|----------|----------|-------------|
| `id`                       | SERIAL   | NOT NULL | Surrogate primary key. |
| `run_id`                   | TEXT     | NOT NULL | Links to the ingest run. |
| `monday_item_id`           | TEXT     | NOT NULL | Monday.com item ID; **UNIQUE**. |
| `monday_board_id`          | TEXT     | YES      | Monday board ID. |
| `monday_url`               | TEXT     | YES      | Link to the item on Monday. |
| `region`                   | TEXT     | YES      | e.g. APAC, NA, Europe/UK, Australia. |
| `campaign_name`            | TEXT     | YES      | |
| `brand_name`               | TEXT     | YES      | |
| `vertical`                 | TEXT     | YES      | |
| `country`                  | TEXT     | YES      | |
| `run_dates`                | TEXT     | YES      | |
| `rfp_summary`              | TEXT     | YES      | |
| `targeting`                | TEXT     | YES      | |
| `trigger_list`             | TEXT     | YES      | Legacy / display field. |
| `any_other_details`        | TEXT     | YES      | |
| `products_to_pitch`        | TEXT     | YES      | Combined product / platform text. |
| `monday_submitted_at`      | TEXT     | YES      | Submission / creation date from Monday (string). |
| `derived_language`         | TEXT     | YES      | Filled by analysis pipeline (OpenAI). |
| `recommended_category`     | TEXT     | YES      | Filled by analysis pipeline. |
| `inventory_status`         | TEXT     | YES      | Filled by analysis pipeline. |
| `available_inventory_count`| INTEGER  | YES      | |
| `p1_channel_count`         | INTEGER  | YES      | |
| `p2_channel_count`         | INTEGER  | YES      | |
| `p3_channel_count`         | INTEGER  | YES      | |
| `media_plan_url`           | TEXT     | YES      | Google Sheet / Drive link for media plan. |
| `context_status`           | TEXT     | YES      | Human-readable context extraction result (✅ / ❌ messages). |
| `recommendation_basis`     | TEXT     | YES      | e.g. input-only vs context-augmented category logic. |
| `error_log`                | TEXT     | YES      | Errors from analysis or ingestion. |
| `inserted_at_utc`          | TEXT     | NOT NULL | ISO-8601 when row inserted. |
| `updated_at_utc`           | TEXT     | YES      | ISO-8601 last update. |

**Primary key:** `id`  
**Unique constraint:** `monday_item_id`

---

## 3. `context_rows`

Rows parsed from the “Context” sheet of media plans (English + local language columns).

| Column             | Type   | Nullable | Description |
|--------------------|--------|----------|-------------|
| `id`               | SERIAL | NOT NULL | Primary key. |
| `run_id`           | TEXT   | YES      | Ingest run id. |
| `monday_item_id`   | TEXT   | YES      | Parent campaign (Monday item id). |
| `monday_board_id`  | TEXT   | YES      | |
| `monday_url`       | TEXT   | YES      | |
| `region`           | TEXT   | YES      | |
| `campaign_name`    | TEXT   | YES      | |
| `brand`            | TEXT   | YES      | |
| `country`          | TEXT   | YES      | |
| `vertical`         | TEXT   | YES      | |
| `brief`            | TEXT   | YES      | Combined brief text. |
| `derived_language` | TEXT   | YES      | |
| `local_language`   | TEXT   | YES      | Detected local list language label. |
| `tactic_en`        | TEXT   | YES      | Tactic (English). |
| `subtactic_en`     | TEXT   | YES      | Sub-tactic (English). |
| `signal_en`        | TEXT   | YES      | Signal (English). |
| `tactic_local`     | TEXT   | YES      | Tactic (local). |
| `subtactic_local`  | TEXT   | YES      | Sub-tactic (local). |
| `signal_local`     | TEXT   | YES      | Signal (local). |
| `inserted_at_utc`  | TEXT   | NOT NULL | ISO-8601. |

**Primary key:** `id`

---

## 4. `access_blocked`

Failures when reading or parsing a media plan; supports optional manual resolution tracking.

| Column             | Type   | Nullable | Description |
|--------------------|--------|----------|-------------|
| `id`               | SERIAL | NOT NULL | Primary key. |
| `run_id`           | TEXT   | YES      | |
| `monday_item_id`   | TEXT   | YES      | |
| `monday_board_id`  | TEXT   | YES      | |
| `monday_url`       | TEXT   | YES      | |
| `region`           | TEXT   | YES      | |
| `campaign_name`    | TEXT   | YES      | |
| `brand`            | TEXT   | YES      | |
| `country`          | TEXT   | YES      | |
| `media_plan_url`   | TEXT   | YES      | |
| `error_message`    | TEXT   | YES      | Technical or short reason. |
| `date_flagged_utc` | TEXT   | NOT NULL | ISO-8601. |
| `resolved_at_utc`  | TEXT   | YES      | |
| `resolved_by`      | TEXT   | YES      | |
| `resolved_note`    | TEXT   | YES      | |

**Primary key:** `id`

---

## 5. `alerts`

Used by the **analysis / presales dashboard** for inventory-related alerts and resolution workflow.

| Column                    | Type     | Nullable | Description |
|---------------------------|----------|----------|-------------|
| `alert_id`                | SERIAL   | NOT NULL | Primary key. |
| `monday_item_id`          | TEXT     | YES      | |
| `monday_url`              | TEXT     | YES      | |
| `region`                  | TEXT     | YES      | |
| `campaign_name`           | TEXT     | YES      | |
| `brand_name`              | TEXT     | YES      | |
| `country`                 | TEXT     | YES      | |
| `derived_language`        | TEXT     | YES      | |
| `products_to_pitch`       | TEXT     | YES      | |
| `monday_run_dates`        | TEXT     | YES      | |
| `monday_submitted_at_utc` | TEXT     | YES      | |
| `recommended_category`    | TEXT     | YES      | |
| `inventory_status`        | TEXT     | NOT NULL | |
| `p1_channel_count`        | INTEGER  | YES      | |
| `p2_channel_count`        | INTEGER  | YES      | |
| `p3_channel_count`        | INTEGER  | YES      | |
| `available_inventory_count` | INTEGER | YES      | |
| `error_log`               | TEXT     | YES      | |
| `date_flagged_utc`        | TEXT     | NOT NULL | |
| `resolved_at_utc`         | TEXT     | YES      | |
| `resolved_by`             | TEXT     | YES      | |
| `resolved_note`           | TEXT     | YES      | |

**Primary key:** `alert_id`

---

## Indexes (created by application)

| Index name              | Table           | Columns |
|-------------------------|-----------------|---------|
| `idx_campaigns_item`    | `campaigns`     | `monday_item_id` |
| `idx_alerts_open`       | `alerts`        | `resolved_at_utc`, `region` |
| `idx_blocked_open`      | `access_blocked`| `resolved_at_utc`, `region` |

IT may add further indexes on `campaigns(region)`, `campaigns(inserted_at_utc)`, etc., if query patterns require it.

---

## Connection expectations

- **Protocol:** PostgreSQL wire protocol (default port 5432 unless overridden).
- **SSL:** Recommended for connections from cloud hosts (Streamlit Cloud, GitHub Actions); use `sslmode=require` (or equivalent) in the connection URI if the server enforces TLS.
- **Application user:** Needs `CREATE` (if app runs migrations), `SELECT`, `INSERT`, `UPDATE`, `DELETE` on the tables above, and usage on sequences for `SERIAL` columns.

---

## Revision

Schema aligned with application code: `init_schema()` in `pipeline.py` (Presales project).  
If the application adds columns later, IT can run the same `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` pattern or rely on the app’s startup migration.
