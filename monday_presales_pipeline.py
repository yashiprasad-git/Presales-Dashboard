import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

try:
    import requests
except Exception:  # pragma: no cover
    requests = None

try:
    from openai import OpenAI
except Exception:  # pragma: no cover
    OpenAI = None


PRESALES_COLUMNS = [
    "Name",
    "Brand Name",
    "Vertical",
    "Country where campaign will run",
    "Products to Pitch",
    "Run dates",
    "RFP Summary - outline objectives of campaign",
    "Targeting - Summary of the type of content you want targeted for the campaign. Has the client indicated any specific targeting? Tentpoles? Sports?",
    "trigger list in local language (check if yes)",
    "Any other details",
]


# ---- Config -----------------------------------------------------------------


@dataclass(frozen=True)
class BoardConfig:
    region: str
    board_id: int
    # Map desired Presales.xlsx columns -> Monday column IDs (strings)
    column_id_map: Dict[str, str]


def load_config(config_path: str) -> List[BoardConfig]:
    """
    Config format (JSON):
    {
      "boards": [
        {
          "region": "APAC Presales",
          "board_id": 123,
          "column_id_map": {
            "Brand Name": "brand",
            "Vertical": "vertical",
            "Country where campaign will run": "country",
            "RFP Summary - outline objectives of campaign": "rfp",
            "Targeting - Summary of the type of content you want targeted for the campaign. Has the client indicated any specific targeting? Tentpoles? Sports?": "targeting",
            "trigger list in local language (check if yes)": "trigger",
            "Any other details": "other"
          }
        }
      ]
    }
    """
    with open(config_path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    boards = []

    # Allow config to use either the exact Presales column names OR snake_case keys.
    snake_to_presales = {
        "brand_name": "Brand Name",
        "vertical": "Vertical",
        "country": "Country where campaign will run",
        "run_dates": "Run dates",
        "rfp_summary": "RFP Summary - outline objectives of campaign",
        "targeting": "Targeting - Summary of the type of content you want targeted for the campaign. Has the client indicated any specific targeting? Tentpoles? Sports?",
        "trigger_list": "trigger list in local language (check if yes)",
        "any_other_details": "Any other details",
        # 'name' is taken from the Monday item name, so we ignore any mapping for it.
        "name": None,
    }

    for b in raw.get("boards", []):
        raw_map = dict(b.get("column_id_map", {}) or {})
        normalized_map: Dict[str, str] = {}

        product_column_name_candidates = [
            "product to pitch",
            "products to pitch",
            "product to propose",
            "products to propose",
            "product proposed",
            "products proposed",
        ]

        def normalize_candidate(s: Any) -> str:
            return " ".join(str(s or "").strip().lower().split())

        # Collect *all* product/platform columns present on this board.
        # Important: some configs use snake_case keys (e.g. products_to_pitch) rather than the UI column title.
        product_col_ids: List[str] = []
        for k, v in raw_map.items():
            if v in (None, ""):
                continue
            key_norm = normalize_candidate(k)
            if key_norm in product_column_name_candidates or key_norm in {
                "products_to_pitch",
                "product_to_pitch",
                "products_to_propose",
                "product_to_propose",
                "products_proposed",
                "product_proposed",
            }:
                product_col_ids.append(str(v))
        # De-dupe while preserving order
        seen: set[str] = set()
        product_col_ids = [x for x in product_col_ids if not (x in seen or seen.add(x))]

        for k, v in raw_map.items():
            if v in (None, ""):
                continue

            # If user provided snake_case keys, translate.
            if k in snake_to_presales:
                presales_col = snake_to_presales[k]
                if presales_col:
                    normalized_map[presales_col] = str(v)
                continue

            # Otherwise assume it is already a Presales column name.
            normalized_map[str(k)] = str(v)

        # Store product column ids for per-item evaluation in the pipeline.
        if product_col_ids:
            normalized_map["__product_col_ids"] = json.dumps(product_col_ids)

        boards.append(
            BoardConfig(
                region=b["region"],
                board_id=int(b["board_id"]),
                column_id_map=normalized_map,
            )
        )
    if not boards:
        raise ValueError("No boards found in config.")
    return boards


# ---- Monday GraphQL ----------------------------------------------------------


MONDAY_API_URL = "https://api.monday.com/v2"


def monday_graphql(api_key: str, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
    if requests is None:
        raise RuntimeError("Missing dependency: requests. Install with `pip install requests`.")
    resp = requests.post(
        MONDAY_API_URL,
        headers={"Authorization": api_key},
        json={"query": query, "variables": variables},
        timeout=60,
    )
    resp.raise_for_status()
    payload = resp.json()
    if "errors" in payload and payload["errors"]:
        raise RuntimeError(f"Monday GraphQL errors: {payload['errors']}")
    return payload["data"]


MONDAY_ITEM_URL = "https://silverpush-global.monday.com/boards/{board_id}/pulses/{item_id}"


def fetch_board_items(api_key: str, board_id: int, limit: int = 500) -> List[Dict[str, Any]]:
    """
    Uses Monday's items_page pagination (cursor).
    Returns list of items, each with id, name, created_at, group, and column_values.
    Note: query_params and cursor cannot be combined — use separate first-page and
    next-page queries.
    """
    first_page_query = """
    query ($board_id: ID!, $limit: Int!) {
      boards(ids: [$board_id]) {
        items_page(limit: $limit) {
          cursor
          items {
            id
            name
            created_at
            group { title }
            column_values { id text value type }
          }
        }
      }
    }
    """
    next_page_query = """
    query ($cursor: String!, $limit: Int!) {
      next_items_page(limit: $limit, cursor: $cursor) {
        cursor
        items {
          id
          name
          created_at
          group { title }
          column_values { id text value type }
        }
      }
    }
    """
    items: List[Dict[str, Any]] = []

    # First page
    data = monday_graphql(api_key, first_page_query, {"board_id": board_id, "limit": limit})
    page = data["boards"][0]["items_page"]
    items.extend(page["items"])
    cursor = page.get("cursor")

    # Subsequent pages
    while cursor:
        data = monday_graphql(api_key, next_page_query, {"cursor": cursor, "limit": limit})
        page = data["next_items_page"]
        items.extend(page["items"])
        cursor = page.get("cursor")

    return items


def board_items_to_presales_df(items: List[Dict[str, Any]], cfg: BoardConfig) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for item in items:
        # Only process items in the "Done" group on the board.
        group_title = ""
        group_obj = item.get("group")
        if isinstance(group_obj, dict):
            group_title = str(group_obj.get("title", "")).strip()
        if group_title.lower() != "done":
            continue

        def _format_monday_value(cv: Dict[str, Any]) -> str:
            """
            Monday column_values often have useful data in `value` (JSON string),
            especially for date/time-range columns where `text` can be empty.
            """
            text = (cv.get("text") or "").strip()
            if text:
                return text

            raw_val = cv.get("value")
            if raw_val in (None, ""):
                return ""

            try:
                data = json.loads(raw_val) if isinstance(raw_val, str) else raw_val
            except Exception:
                return str(raw_val).strip()

            cv_type = (cv.get("type") or "").strip().lower()

            if isinstance(data, dict):
                # timerange / date formats vary; handle common shapes
                # Some accounts store timeranges under different keys
                start_keys = ["from", "start", "start_date", "from_date"]
                end_keys = ["to", "end", "end_date", "to_date"]

                def first_str(keys: List[str]) -> str:
                    for k in keys:
                        v = data.get(k)
                        if v is None:
                            continue
                        if isinstance(v, dict):
                            # Sometimes nested like {"date": "..."}
                            if "date" in v and v["date"]:
                                return str(v["date"]).strip()
                            continue
                        s = str(v).strip()
                        if s and s.lower() != "none":
                            return s
                    return ""

                if cv_type in {"timerange", "date", "timeline"} or any(k in data for k in start_keys + end_keys):
                    start = first_str(start_keys)
                    end = first_str(end_keys)
                    if start and end and start != end:
                        return f"{start} - {end}"
                    return start or end

                if "date" in data and data.get("date"):
                    return str(data.get("date") or "").strip()

                if "dates" in data and isinstance(data["dates"], list):
                    parts = []
                    for d in data["dates"]:
                        if isinstance(d, dict) and d.get("date"):
                            parts.append(str(d["date"]).strip())
                    return ", ".join([p for p in parts if p])

                # dropdown / multi_select: value often has "labels" (list) or "label" or "selected_value"
                if cv_type in ("dropdown", "multi_select") or "dropdown" in cv_type or "multi_select" in cv_type:
                    labels = data.get("labels")
                    if isinstance(labels, list) and labels:
                        return ", ".join(str(x).strip() for x in labels if x is not None and str(x).strip())
                    label = data.get("label")
                    if label is not None and str(label).strip():
                        return str(label).strip()
                    selected = data.get("selected_value")
                    if selected is not None and str(selected).strip():
                        return str(selected).strip()

            return str(data).strip()

        col_values = {
            cv["id"]: _format_monday_value(cv) for cv in item.get("column_values", [])
        }

        # Campaign filtering logic when multiple "product" columns exist on a board:
        # - Include a campaign if ANY product column contains "youtube" or "mirror"/"mirrors" (case-insensitive).
        # - Include a campaign if ALL product columns are blank (needs manual follow-up).
        # - Exclude a campaign if one product column is non-blank but none mention youtube/mirror.
        def _mentions_youtube_or_mirror(v: str) -> bool:
            s = (v or "").strip().lower()
            return bool(s) and ("youtube" in s or "mirror" in s)

        product_col_ids: List[str] = []
        raw_ids = cfg.column_id_map.get("__product_col_ids")
        if raw_ids:
            try:
                decoded = json.loads(raw_ids)
                if isinstance(decoded, list):
                    product_col_ids = [str(x) for x in decoded if str(x).strip()]
            except Exception:
                product_col_ids = []

        product_vals: List[str] = []
        for cid in product_col_ids:
            product_vals.append(str(col_values.get(cid, "") or "").strip())

        if product_vals:
            any_nonblank = any(v.strip() for v in product_vals)
            any_match = any(_mentions_youtube_or_mirror(v) for v in product_vals)
            if any_nonblank and not any_match:
                continue

        row: Dict[str, Any] = {c: "" for c in PRESALES_COLUMNS}
        row["Name"] = item.get("name", "") or ""
        row["Monday_Submitted_At"] = item.get("created_at", "") or ""
        item_id = str(item.get("id", "") or "")
        row["Monday_Item_ID"] = item_id
        row["Monday_URL"] = MONDAY_ITEM_URL.format(board_id=cfg.board_id, item_id=item_id) if item_id else ""

        # Fill mapped columns (show product field as-is from Monday; do not auto-fill blank).
        for presales_col, monday_col_id in cfg.column_id_map.items():
            if presales_col.startswith("__"):
                continue
            if presales_col in row:
                row[presales_col] = col_values.get(monday_col_id, "") or ""

        # Display the product field as the filled value(s) across product columns.
        if product_vals:
            filled = [v for v in product_vals if v.strip()]
            if filled:
                seen_disp: set[str] = set()
                uniq: List[str] = []
                for v in filled:
                    if v not in seen_disp:
                        uniq.append(v)
                        seen_disp.add(v)
                row["Products to Pitch"] = " | ".join(uniq)
            else:
                row["Products to Pitch"] = ""

        rows.append(row)

    # Keep the standard presales columns plus extra metadata columns
    df = pd.DataFrame(rows, columns=PRESALES_COLUMNS + ["Monday_Submitted_At", "Monday_Item_ID", "Monday_URL"])

    # Clean: remove rows where EVERYTHING is blank (including Name).
    # Keep rows with a campaign Name even if other columns are not mapped yet.
    df = df.dropna(how="all")
    all_blank_mask = df.fillna("").astype(str).apply(
        lambda r: "".join([str(v).strip() for v in r.values]).strip() == "",
        axis=1,
    )
    df = df.loc[~all_blank_mask]
    return df.reset_index(drop=True)


# ---- Inventory + Recommendations (ported from presales_category_recommender) -


def _normalize_col_name(name: str) -> str:
    return "".join(ch for ch in str(name).strip().lower() if ch.isalnum())


def _resolve_inventory_columns(df: pd.DataFrame) -> Dict[str, str]:
    norm_to_actual = {_normalize_col_name(c): c for c in df.columns}

    def pick(*candidates: str) -> Optional[str]:
        for cand in candidates:
            actual = norm_to_actual.get(_normalize_col_name(cand))
            if actual:
                return actual
        return None

    resolved = {
        "channelid": pick("channelid", "channel_id", "channel id"),
        "country": pick("country"),
        "category": pick("category"),
        "description_language": pick("description language", "description languge"),
        "resource_language": pick("resource language"),
        "model_channel_language": pick("model channel language"),
        "model_video_language": pick("model video language"),
    }

    missing = [
        k
        for k, v in resolved.items()
        if v is None and k in {"category", "model_video_language"}
    ]
    if missing:
        raise ValueError(
            f"Inventory file is missing required columns: {missing}. Found columns: {df.columns.tolist()}"
        )
    return resolved  # type: ignore[return-value]


def _parse_lang_codes(value: Any) -> List[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    s = str(value).strip()
    if not s:
        return []
    return [c.strip().lower() for c in s.split(",") if c.strip()]


def _any_lang_match(value: Any, required_codes: Iterable[str]) -> bool:
    codes = _parse_lang_codes(value)
    req = [str(rc).strip().lower() for rc in required_codes if str(rc).strip()]
    return any(rc in codes for rc in req)


language_code_map = {
    "English": ["en"],
    "French": ["fr"],
    "German": ["de"],
    "Dutch": ["nl"],
    "Spanish": ["es"],
    "Turkish": ["tr"],
    "Catalan": ["ca"],
    "Galician": ["gl"],
    "Italian": ["it"],
    "Hindi": ["hi", "hi-latn"],
    "English/Hindi": ["en", "hi", "hi-latn"],
    "Vietnamese": ["vi"],
    "Malay": ["ms"],
    "Chinese": ["zh", "zh-hans", "zh-hant"],
    "Thai": ["th"],
    "Bahasa": ["id"],
    "Indonesian": ["id"],
    "Filipino": ["fil", "tl"],
    "Marathi": ["mr"],
    "Kannada": ["kn"],
    "Telugu": ["te"],
    "Bengali": ["bn"],
    "Tamil": ["ta"],
    "Danish": ["da"],
    "Norwegian": ["no", "nb", "nn"],
    "Swedish": ["sv"],
    "Finnish": ["fi"],
    "Icelandic": ["is"],
}


geo_language_map = {
    # Europe / NA
    "France": ["French", "English", "Spanish"],
    "Germany": ["German", "English", "Turkish"],
    "Netherlands": ["Dutch", "English"],
    "Spain": ["Spanish", "Catalan", "Galician"],
    "Italy": ["Italian"],
    "United Kingdom": ["English"],
    "UK": ["English"],
    "Europe/UK": ["English"],
    "US": ["English"],
    "USA": ["English"],
    "United States": ["English"],

    # Nordics (bucket)
    "Denmark": ["Danish"],
    "Norway": ["Norwegian"],
    "Sweden": ["Swedish"],
    "Finland": ["Finnish"],
    "Iceland": ["Icelandic"],
    "Nordics": ["Danish", "Norwegian", "Swedish", "Finnish", "Icelandic", "English"],

    # APAC
    "Vietnam": ["Vietnamese"],
    "Malaysia": ["Malay", "Chinese", "English"],
    "Thailand": ["Thai"],
    "Indonesia": ["Bahasa"],
    "India": ["English", "Hindi"],
    "Singapore": ["English", "Chinese"],
    "Philippines": ["Filipino", "English"],
}


_language_keyword_map = {
    "marathi": "Marathi",
    "kannada": "Kannada",
    "telugu": "Telugu",
    "bengali": "Bengali",
    "tamil": "Tamil",
    "hindi": "Hindi",
    "english": "English",
}


def _normalize_geo_name(geo_raw: Any) -> Optional[str]:
    g = str(geo_raw or "").strip().lower()
    if not g:
        return None
    if "vietnam" in g:
        return "Vietnam"
    if "malaysia" in g:
        return "Malaysia"
    if "thailand" in g or "thai" in g:
        return "Thailand"
    if "indonesia" in g or "indo" in g:
        return "Indonesia"
    if "singapore" in g:
        return "Singapore"
    if "phil" in g:  # Philippines / Philipines / etc.
        return "Philippines"
    if "nether" in g or g == "nl" or "holland" in g:
        return "Netherlands"
    if "denmark" in g or g == "dk":
        return "Denmark"
    if "norway" in g or g == "no":
        return "Norway"
    if "sweden" in g or g == "se":
        return "Sweden"
    if "finland" in g or g == "fi":
        return "Finland"
    if "iceland" in g or g == "is":
        return "Iceland"
    if "nordic" in g or "nordics" in g:
        return "Nordics"
    if "united kingdom" in g or g == "uk" or "europe/uk" in g:
        return "UK"
    if g in {"us", "usa", "united states", "united states of america"}:
        return "US"
    if "india" in g:
        return "India"
    return str(geo_raw).strip()


def derive_languages(geo: Any, brief_text: Any) -> List[str]:
    norm_geo = _normalize_geo_name(geo)
    base = geo_language_map.get(norm_geo, ["English"])
    derived = list(base)
    if str(norm_geo or "").lower() == "india":
        text = str(brief_text or "").lower()
        for kw, lang in _language_keyword_map.items():
            if kw in text and lang not in derived:
                derived.append(lang)
    return derived


def language_list_to_codes(language_list: List[str]) -> List[str]:
    codes: List[str] = []
    for lang in language_list or []:
        for c in language_code_map.get(str(lang).strip(), []):
            c = str(c).strip().lower()
            if c and c not in codes:
                codes.append(c)
    return codes


brand_category_map = {
    "airbnb": ["Travel"],
}

vertical_category_map = {
    "foodbeverage": ["FoodBeverage"],
    "food&beverage": ["FoodBeverage"],
    "foodandbeverage": ["FoodBeverage"],
    "beverages": ["FoodBeverage"],
    "travel": ["Travel"],
    "sports": ["Sports"],
    "automotive": ["Automobile"],
    "automobiles": ["Automobile"],
    "finance": ["BusinessFinance"],
    "businessfinance": ["BusinessFinance"],
    "shopping": ["Shopping"],
}


def analyze_inventory_availability(
    inventory_df: pd.DataFrame,
    inv_cols: Dict[str, str],
    categories: Any,
    required_language_codes: List[str],
) -> Dict[str, Any]:
    if not categories or not required_language_codes:
        return {"p1": 0, "p2": 0, "p3": 0, "total": 0, "status": "Nil"}

    if isinstance(categories, str):
        categories_list = [c.strip() for c in categories.split(",") if c.strip()]
    else:
        categories_list = list(categories)

    clean_categories = [
        c for c in categories_list if c and c != "NO_MATCH" and not str(c).startswith("ERROR")
    ]
    if not clean_categories:
        return {"p1": 0, "p2": 0, "p3": 0, "total": 0, "status": "Nil"}

    df = inventory_df
    base_mask = df[inv_cols["category"]].fillna("").isin(clean_categories)

    channel_key = inv_cols.get("channelid")
    channel_series = df[channel_key].fillna("") if channel_key else df.index.to_series()

    mv = df[inv_cols["model_video_language"]]
    rl = df[inv_cols["resource_language"]] if inv_cols.get("resource_language") else pd.Series([""] * len(df))
    mcl = df[inv_cols["model_channel_language"]] if inv_cols.get("model_channel_language") else pd.Series([""] * len(df))
    dl = df[inv_cols["description_language"]] if inv_cols.get("description_language") else pd.Series([""] * len(df))

    mv_match = mv.fillna("").apply(lambda v: _any_lang_match(v, required_language_codes))
    rl_match = rl.fillna("").apply(lambda v: _any_lang_match(v, required_language_codes))
    mcl_match = mcl.fillna("").apply(lambda v: _any_lang_match(v, required_language_codes))
    dl_match = dl.fillna("").apply(lambda v: _any_lang_match(v, required_language_codes))
    rl_blank = rl.fillna("").astype(str).str.strip().eq("")

    p1_mask = base_mask & mv_match & rl_match & mcl_match
    p2_mask = base_mask & mv_match & rl_match
    p3_mask = base_mask & rl_blank & mv_match & mcl_match & dl_match

    p1_channels = set(channel_series[p1_mask].tolist())
    p2_channels = set(channel_series[p2_mask].tolist()) - p1_channels
    p3_channels = set(channel_series[p3_mask].tolist()) - p1_channels - p2_channels

    def _count_nonblank(s: set) -> int:
        return len([c for c in s if str(c).strip()])

    p1 = _count_nonblank(p1_channels)
    p2 = _count_nonblank(p2_channels)
    p3 = _count_nonblank(p3_channels)
    total = p1 + p2 + p3

    if total == 0:
        status = "Nil"
    elif 1 <= total <= 100:
        status = "Low"
    elif 101 <= total <= 500:
        status = "Medium"
    else:
        status = "Okay"

    return {"p1": p1, "p2": p2, "p3": p3, "total": total, "status": status}


def _get_brand_categories(brand: Any) -> List[str]:
    if not brand:
        return []
    return brand_category_map.get(str(brand).strip().lower(), [])


def _get_vertical_categories(vertical: Any) -> List[str]:
    if not vertical:
        return []
    key = str(vertical).strip().lower()
    compact = "".join(ch for ch in key if ch.isalnum() or ch in {"&"})
    return vertical_category_map.get(compact, []) or vertical_category_map.get(
        "".join(ch for ch in key if ch.isalnum()),
        [],
    )


def _merge_categories(model_output: str, implied_cats: List[str], available_categories: List[str]) -> str:
    available_set = {str(c).strip() for c in available_categories}

    if not model_output or str(model_output).strip() == "NO_MATCH":
        model_list: List[str] = []
    else:
        model_list = [x.strip() for x in str(model_output).split(",") if x.strip()]

    filtered_model = [c for c in model_list if c in available_set]
    filtered_implied = [c for c in (implied_cats or []) if str(c).strip() in available_set]

    merged: List[str] = []
    seen = set()
    for c in filtered_model + filtered_implied:
        if c not in seen:
            merged.append(c)
            seen.add(c)

    return "NO_MATCH" if not merged else ", ".join(merged)


def derive_categories_with_openai(
    client: Any,
    brief: str,
    geo: str,
    language: str,
    brand: Any,
    vertical: Any,
    category_list: List[str],
) -> str:
    brand_categories = _get_brand_categories(brand)
    vertical_categories = _get_vertical_categories(vertical)
    implied_categories: List[str] = []
    for c in (brand_categories + vertical_categories):
        if c not in implied_categories:
            implied_categories.append(c)

    system_prompt = """
You are an expert in contextual targeting for YouTube campaigns.

You MUST choose ALL relevant categories from the provided list, and you MUST
include any Brand-Implied and Vertical-Implied Categories (if they exist in the provided list).

Think in two layers:
- Core categories that match the brand, product, and vertical (e.g., FoodBeverage for coffee brands,
  BusinessFinance for cards/credits, TELCO-related for connectivity products).
- Supporting/contextual categories that reflect how, where, and why the product is used, such as:
  Travel (travel rewards, trip planning, hospitality), Shopping, Entertainment, Sports, Fitness,
  HealthWellness, StyleFashion, ScienceTechnology, Kids, etc., when these are clearly implied by
  the brief, targeting, or benefits.

Examples of supporting logic:
- A credit card with travel/lounge benefits should include Travel in addition to BusinessFinance.
- A telco or connectivity product enabling streaming, gaming, or OTT should include Entertainment or Gaming.
- A health or personal care product should include HealthWellness alongside Beauty/StyleFashion if relevant.
- A food or beverage consumed during leisure moments can include Entertainment together with FoodBeverage.

Rules:
1. Choose ONE or MORE categories from the provided list (core + supporting, when justified).
2. Do NOT invent new category names outside the provided list.
3. Return the answer as a comma-separated list of category names, ordered from most to least relevant.
4. If no match is appropriate, return exactly 'NO_MATCH'.
""".strip()

    user_prompt = f"""
Brand:
{brand}

Vertical:
{vertical}

Campaign Brief:
{brief}

Target Country:
{geo}

Target Language:
{language}

Brand-Implied Categories (MUST include if relevant and present in list):
{brand_categories}

Vertical-Implied Categories (MUST include if relevant and present in list):
{vertical_categories}

Available Categories:
{category_list}
""".strip()

    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0,
    )
    model_output = resp.choices[0].message.content.strip()
    return _merge_categories(model_output, implied_categories, category_list)


def run_recommendations_for_region(
    presales_df: pd.DataFrame,
    inventory_df: pd.DataFrame,
    inventory_cols: Dict[str, str],
    category_list: List[str],
    client: Any,
) -> pd.DataFrame:
    recommended_categories: List[str] = []
    derived_languages: List[str] = []
    p1_counts: List[int] = []
    p2_counts: List[int] = []
    p3_counts: List[int] = []
    totals: List[int] = []
    statuses: List[str] = []
    error_log: List[str] = []

    for _, row in presales_df.iterrows():
        brand = row.get("Brand Name", "")
        vertical = row.get("Vertical", "")
        geo = row.get("Country where campaign will run", "")

        brief_text = f"""
RFP Summary: {row.get('RFP Summary - outline objectives of campaign', '')}
Targeting Details: {row.get('Targeting - Summary of the type of content you want targeted for the campaign. Has the client indicated any specific targeting? Tentpoles? Sports?', '')}
Additional Details: {row.get('Any other details', '')}
""".strip()
        derived_language_list = derive_languages(geo, brief_text)
        language_for_prompt = ", ".join(derived_language_list)
        derived_languages.append(language_for_prompt)

        try:
            cats = derive_categories_with_openai(
                client=client,
                brief=brief_text,
                geo=str(geo),
                language=language_for_prompt,
                brand=brand,
                vertical=vertical,
                category_list=category_list,
            )
            error_log.append("")
        except Exception as e:
            cats = f"ERROR: {e}"
            error_log.append(str(e))

        recommended_categories.append(cats)

        required_codes = language_list_to_codes(derived_language_list) or ["en"]
        inv = analyze_inventory_availability(
            inventory_df=inventory_df,
            inv_cols=inventory_cols,
            categories=cats,
            required_language_codes=required_codes,
        )
        p1_counts.append(inv["p1"])
        p2_counts.append(inv["p2"])
        p3_counts.append(inv["p3"])
        totals.append(inv["total"])
        statuses.append(inv["status"])

    out = presales_df.copy()
    out["Derived_Language"] = derived_languages
    out["Recommended_Category"] = recommended_categories
    out["P1_Channel_Count"] = p1_counts
    out["P2_Channel_Count"] = p2_counts
    out["P3_Channel_Count"] = p3_counts
    out["Available_Inventory_Count"] = totals
    out["Inventory_Status"] = statuses
    out["Error_Log"] = error_log
    return out


# ---- IO helpers --------------------------------------------------------------


def load_inventory(path: str) -> Tuple[pd.DataFrame, Dict[str, str], List[str]]:
    if path.lower().endswith(".csv"):
        try:
            df = pd.read_csv(path, encoding="utf-8")
        except UnicodeDecodeError:
            df = pd.read_csv(path, encoding="latin1")
    else:
        df = pd.read_excel(path)

    df.columns = df.columns.str.strip()
    inv_cols = _resolve_inventory_columns(df)
    category_col = inv_cols["category"]
    categories = df[category_col].dropna().astype(str).unique().tolist()
    return df, inv_cols, categories


def write_multisheet_excel(path: str, sheets: Dict[str, pd.DataFrame]) -> None:
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for name, df in sheets.items():
            # Excel sheet name rules: max 31 chars and no: : \\ / ? * [ ]
            safe_name = str(name)
            for ch in [":", "\\", "/", "?", "*", "[", "]"]:
                safe_name = safe_name.replace(ch, "-")
            safe_name = safe_name.strip()[:31]
            df.to_excel(writer, sheet_name=safe_name, index=False)


FIRST_RUN_SINCE = "2026-03-20"  # Hard floor: never process campaigns created before this date


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("config", help="Path to monday_config.json")
    parser.add_argument("inventory", help="Path to inventory file")
    parser.add_argument(
        "--since",
        default=None,
        help="Only include campaigns created on/after this date (YYYY-MM-DD). "
             f"Defaults to last successful run date, or {FIRST_RUN_SINCE} on first run.",
    )
    args = parser.parse_args()

    config_path = args.config
    inventory_path = args.inventory
    since_date_str: str = args.since if args.since else FIRST_RUN_SINCE

    try:
        since_dt = datetime.strptime(since_date_str, "%Y-%m-%d")
    except ValueError:
        print(f"Invalid --since date '{since_date_str}', falling back to {FIRST_RUN_SINCE}")
        since_dt = datetime.strptime(FIRST_RUN_SINCE, "%Y-%m-%d")

    print(f"Processing campaigns created on/after: {since_dt.date()}")

    monday_key = os.getenv("MONDAY_API_KEY")
    if not monday_key:
        raise RuntimeError("MONDAY_API_KEY is not set.")

    openai_key = os.getenv("OPENAI_API_KEY")
    if not openai_key:
        raise RuntimeError("OPENAI_API_KEY is not set.")
    if OpenAI is None:
        raise RuntimeError("Missing dependency: openai. Install with `pip install openai`.")

    boards = load_config(config_path)

    inventory_df, inventory_cols, category_list = load_inventory(inventory_path)

    client = OpenAI(api_key=openai_key)

    cleaned_sheets: Dict[str, pd.DataFrame] = {}
    recommended_sheets: Dict[str, pd.DataFrame] = {}

    for b in boards:
        print(f"Fetching Monday board: {b.region}")
        items = fetch_board_items(monday_key, b.board_id)
        df = board_items_to_presales_df(items, b)

        # Filter by since_date using the Monday_Submitted_At (created_at) column
        if "Monday_Submitted_At" in df.columns and not df.empty:
            def _parse_created(v: Any) -> Optional[datetime]:
                try:
                    s = str(v or "").strip()
                    if not s:
                        return None
                    return datetime.fromisoformat(s.replace("Z", "+00:00")).replace(tzinfo=None)
                except Exception:
                    return None

            created_dts = df["Monday_Submitted_At"].apply(_parse_created)
            mask = created_dts.apply(lambda d: d is None or d >= since_dt)
            before_count = len(df)
            df = df[mask].reset_index(drop=True)
            skipped = before_count - len(df)
            if skipped:
                print(f"  Skipped {skipped} campaign(s) created before {since_dt.date()}")
        cleaned_sheets[b.region] = df

        print(f"Running recommendations for: {b.region} (rows={len(df)})")
        rec_df = run_recommendations_for_region(
            presales_df=df,
            inventory_df=inventory_df,
            inventory_cols=inventory_cols,
            category_list=category_list,
            client=client,
        )
        recommended_sheets[b.region] = rec_df

    ts = datetime.now().strftime("%Y%m%d_%H%M")
    cleaned_path = f"Monday_Presales_Cleaned_{ts}.xlsx"
    out_path = f"Monday_Presales_Recommendations_{ts}.xlsx"

    write_multisheet_excel(cleaned_path, cleaned_sheets)
    write_multisheet_excel(out_path, recommended_sheets)

    print(f"✅ Cleaned presales exported: {cleaned_path}")
    print(f"✅ Recommendations exported: {out_path}")


if __name__ == "__main__":
    main()

