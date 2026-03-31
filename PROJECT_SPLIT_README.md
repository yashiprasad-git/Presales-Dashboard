# Project split: MDB vs analysis

A **full backup** of the folder before this split was saved as:

`Desktop/presales_backup_before_split_YYYYMMDD.tar.gz`

---

## Layout

| Folder | Purpose | Secrets |
|--------|---------|---------|
| **`mdb/`** | Monday → PostgreSQL (`db_updater.py`), monitor dashboard (`monitor_dashboard.py`), `mdb_core.py` (no OpenAI) | `DATABASE_URL`, `MONDAY_API_KEY` only — see `mdb/.streamlit/secrets.toml.example` |
| **`analysis/`** | Category + inventory pipeline (`pipeline.py`), presales dashboard (`presales_dashboard.py`) | Same + **`OPENAI_API_KEY`** — see `analysis/.streamlit/secrets.toml.example` |

Each folder has its **own copy** of `monday_config.json` and a **secrets template** — duplicate values when you configure locally or on Streamlit Cloud.

---

## Run commands (from repo root)

**MDB — DB updater**

```bash
python db_updater.py
# or
cd mdb && python db_updater.py monday_config.json
```

**MDB — monitor dashboard**

```bash
streamlit run mdb/monitor_dashboard.py
```

**Analysis — presales dashboard**

```bash
streamlit run analysis/presales_dashboard.py
```

**Analysis — full pipeline (OpenAI + inventory)**

```bash
python analysis/pipeline.py analysis/monday_config.json /path/to/inventory.xlsx
```

---

## GitHub Actions

The workflow `.github/workflows/db_updater.yml` installs **`mdb/requirements.txt`** and runs the updater with **`working-directory: mdb`**. No OpenAI dependency in CI.

---

## Streamlit Cloud

- **MDB monitor app:** main file = `mdb/monitor_dashboard.py`  
- **Analysis app:** main file = `analysis/presales_dashboard.py`  

Set secrets per app from the matching `.streamlit/secrets.toml.example`.

---

## Legacy files at repo root

`pipeline.py`, `presales_dashboard.py`, and `mdb_core` logic were split; the **canonical copies** for ongoing development are under `analysis/` and `mdb/`. You may remove duplicate root files later once you confirm nothing else references them.
