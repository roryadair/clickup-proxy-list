import os
import re
import math
import json
from typing import Any, Dict, Iterable, List, Mapping, Optional

import requests
import pandas as pd
import streamlit as st
from dateutil import parser as dtparse

# -----------------------------
# Streamlit Page Config
# -----------------------------
st.set_page_config(page_title="ClickUp Data Extractor", layout="wide")

st.title("ClickUp Data Extractor")
st.caption("Fund Solution Workspace → ACTIVE Proxy Efforts (integrated fuzzy label parsing)")

# -----------------------------
# Secrets / Config
# -----------------------------
# Expect these in .streamlit/secrets.toml
# [clickup]
# token = "YOUR_CLICKUP_TOKEN"
# list_id = "1234567890"

CLICKUP_TOKEN = st.secrets.get("clickup", {}).get("token") if hasattr(st, "secrets") else os.getenv("CLICKUP_TOKEN")
DEFAULT_LIST_ID = st.secrets.get("clickup", {}).get("list_id") if hasattr(st, "secrets") else os.getenv("CLICKUP_LIST_ID")

if not CLICKUP_TOKEN:
    st.error("Missing ClickUp token. Please add it to Streamlit secrets as [clickup].token or set CLICKUP_TOKEN env var.")
    st.stop()

list_id = DEFAULT_LIST_ID or st.text_input("ClickUp List ID", value="", help="If not provided in secrets, enter the List ID here.")
if not list_id:
    st.info("Provide a List ID to continue.")
    st.stop()

# -----------------------------
# HTTP Client
# -----------------------------
API_BASE = "https://api.clickup.com/api/v2"
HEADERS = {
    "Authorization": CLICKUP_TOKEN,
    "Content-Type": "application/json"
}


def fetch_tasks_from_list(list_id: str, include_closed: bool = True) -> List[Dict[str, Any]]:
    """Fetch all tasks from a ClickUp list with pagination."""
    tasks: List[Dict[str, Any]] = []
    page = 0
    per_page = 100
    while True:
        params = {
            "page": page,
            "include_closed": str(include_closed).lower(),
            "subtasks": "true",
        }
        url = f"{API_BASE}/list/{list_id}/task"
        resp = requests.get(url, headers=HEADERS, params=params, timeout=60)
        if resp.status_code != 200:
            raise RuntimeError(f"ClickUp API error {resp.status_code}: {resp.text}")
        data = resp.json() or {}
        chunk = data.get("tasks", [])
        tasks.extend(chunk)
        # Pagination: stop when returned less than per_page
        if len(chunk) < per_page:
            break
        page += 1
    return tasks

# -----------------------------
# Fuzzy label parsing utilities (integrated)
# -----------------------------
LABEL_PATTERNS: Dict[str, re.Pattern] = {
    # Use strict word boundaries to avoid collisions like "RECORD DATE RANGE"
    "record_date": re.compile(r"\bRECORD\s*DATE\b", re.IGNORECASE),
    "meeting_date": re.compile(r"\bMEETING\s*DATE\b", re.IGNORECASE),
    "job_number": re.compile(r"\bJOB\s*NUMBER\b", re.IGNORECASE),
    # Add as needed:
    # "ics_job": re.compile(r"\bICS\s*JOB\b", re.IGNORECASE),
    # "mc_number": re.compile(r"\bMC\b", re.IGNORECASE),
}


def _clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def _parse_date_maybe(s: str) -> Optional[str]:
    if not s:
        return None
    try:
        dt = dtparse.parse(s, fuzzy=True)
        return dt.date().isoformat()
    except Exception:
        return None


def _label_matches(key: str, label_re: re.Pattern) -> bool:
    return bool(label_re.search(key))


def _value_from_key_trailer(key: str, label_re: re.Pattern) -> Optional[str]:
    m = re.search(rf"{label_re.pattern}[:\-\s]*(.+)$", key, re.IGNORECASE)
    if m:
        return _clean_text(m.group(1))
    return None


def extract_field_value(
    items: Iterable[Mapping[str, Any]],
    label_key: str,
    prefer_parsed_date: bool = True,
) -> Optional[str]:
    """Generic extractor using fuzzy label matching.
    Prefers the associated field 'value'; falls back to extracting from the key itself.
    Optionally parses dates into ISO.
    """
    label_re = LABEL_PATTERNS[label_key]

    for item in items:
        key = str(item.get("name", "") or "")
        val = item.get("value", "")
        val_str = "" if val is None else str(val)

        if not _label_matches(key, label_re):
            continue

        candidate = _clean_text(val_str)
        if not candidate:
            candidate = _value_from_key_trailer(key, label_re) or ""
        candidate = _clean_text(candidate)
        if not candidate:
            continue

        if prefer_parsed_date and label_key.endswith("_date"):
            parsed = _parse_date_maybe(candidate)
            if parsed:
                return parsed
        return candidate

    return None

# -----------------------------
# Domain-specific helpers
# -----------------------------
CODE_PATTERN = re.compile(r"\b([SPZ]\d{5,})\b", re.IGNORECASE)


def extract_all_codes(text: str) -> str:
    """Capture multiple S##### / P##### / Z##### codes, unique, keep order of first appearance."""
    if not text:
        return ""
    matches = CODE_PATTERN.findall(text)
    if not matches:
        return ""
    # preserve order while deduping
    seen = set()
    ordered = []
    for m in matches:
        u = m.upper()
        if u not in seen:
            seen.add(u)
            ordered.append(u)
    return ", ".join(ordered)


# -----------------------------
# Build dataframe from tasks
# -----------------------------

def tasks_to_dataframe(tasks: List[Dict[str, Any]]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []

    for t in tasks:
        name = t.get("name", "")
        desc = t.get("text_content") or t.get("description") or ""
        custom_fields = t.get("custom_fields", [])

        # Fuzzy-extracted fields
        record_date = extract_field_value(custom_fields, "record_date")
        meeting_date = extract_field_value(custom_fields, "meeting_date")
        job_number = extract_field_value(custom_fields, "job_number", prefer_parsed_date=False)

        # Multi-code extraction from name + description
        codes = extract_all_codes(" ".join([name or "", desc or ""]))

        rows.append({
            "Task ID": t.get("id"),
            "Task Name": name,
            "URL": t.get("url"),
            "Status": (t.get("status") or {}).get("status"),
            "Assignees": ", ".join([a.get("username") or a.get("email") or a.get("id") for a in t.get("assignees", [])]) if t.get("assignees") else "",
            "Record Date": record_date,
            "Meeting Date": meeting_date,
            "Job Number": job_number,
            "Codes (S/P/Z)": codes,
        })

    df = pd.DataFrame(rows)

    # Remove the known non-entry row "PROJECT LIST TEMPLATE"
    if not df.empty and "Task Name" in df.columns:
        df = df[df["Task Name"].str.strip().str.upper() != "PROJECT LIST TEMPLATE"].copy()

    # Optional: sort
    if "Record Date" in df.columns:
        # Safe sort: parse dates, fallback to NaT
        df["Record Date (sort)"] = pd.to_datetime(df["Record Date"], errors="coerce")
        df = df.sort_values(["Record Date (sort)", "Task Name"], na_position="last").drop(columns=["Record Date (sort)"])

    return df.reset_index(drop=True)

# -----------------------------
# Run
# -----------------------------
with st.spinner("Fetching tasks from ClickUp…"):
    try:
        tasks = fetch_tasks_from_list(list_id)
    except Exception as e:
        st.exception(e)
        st.stop()

st.success(f"Fetched {len(tasks)} tasks.")

# Build and show dataframe
with st.spinner("Parsing tasks…"):
    df = tasks_to_dataframe(tasks)

st.dataframe(df, use_container_width=True)

# Download buttons
@st.cache_data
def df_to_csv_bytes(dataframe: pd.DataFrame) -> bytes:
    return dataframe.to_csv(index=False).encode("utf-8")

csv_bytes = df_to_csv_bytes(df)
st.download_button("Download CSV", data=csv_bytes, file_name="clickup_tasks.csv", mime="text/csv")

st.caption("Tip: Add/adjust LABEL_PATTERNS in this file to catch other fuzzy labels (e.g., ICS Job, MC, etc.) while always preferring the associated field value when present.")
