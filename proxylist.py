import os
import re
import json
from typing import Any, Dict, List, Mapping, Optional

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
# Secrets / Config  (supports both styles)
# -----------------------------
CLICKUP_TOKEN = (
    (st.secrets.get("CLICKUP_TOKEN") if hasattr(st, "secrets") else None)
    or (st.secrets.get("clickup", {}).get("token") if hasattr(st, "secrets") else None)
    or os.getenv("CLICKUP_TOKEN")
)

DEFAULT_LIST_ID = (
    (st.secrets.get("CLICKUP_LIST_ID") if hasattr(st, "secrets") else None)
    or (st.secrets.get("clickup", {}).get("list_id") if hasattr(st, "secrets") else None)
    or os.getenv("CLICKUP_LIST_ID")
)

if not CLICKUP_TOKEN:
    st.error(
        "Missing ClickUp token. Set `CLICKUP_TOKEN = \"...\"` in secrets, or under "
        "[clickup] token = \"...\", or set the CLICKUP_TOKEN environment variable."
    )
    st.stop()

# -------- List ID input / default --------
list_id = (DEFAULT_LIST_ID or os.getenv("CLICKUP_LIST_ID") or "").strip()

# Let the user override or supply it if missing
list_id = st.text_input(
    "ClickUp List ID",
    value=list_id,
    help="Paste the ClickUp List ID (e.g., 901234567) if not already set in secrets.",
).strip()

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
        if len(chunk) < per_page:
            break
        page += 1
    return tasks

# -----------------------------
# Fuzzy label parsing utilities
# -----------------------------
LABEL_PATTERNS: Dict[str, re.Pattern] = {
    "record_date": re.compile(r"\b(RECORD\s*DATE|REC\.?\s*DATE|RCD\s*DATE)\b", re.IGNORECASE),
    "meeting_date": re.compile(r"\b(MEETING\s*DATE|MTG\s*DATE)\b", re.IGNORECASE),
}

# Fix: removed stray "|S|" so months like September don't get blocked
BAD_REMAINDER = re.compile(r"^(RANGE|WINDOW|UNTIL|THRU|THROUGH|TO)\b", re.IGNORECASE)


def _clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _parse_date_maybe(s: str) -> Optional[str]:
    if not s:
        return None
    try:
        dt = dtparse.parse(s, fuzzy=True)
        return dt.date().isoformat()
    except Exception:
        return None


def _label_matches(key: str, label_re: re.Pattern) -> bool:
    return bool(label_re.search(key or ""))


def extract_trailing_after_label(label_key: str, text: str) -> str:
    pat = LABEL_PATTERNS[label_key]
    m = re.search(rf"{pat.pattern}[:\\-\\s]*(.+)$", text or "", re.IGNORECASE)
    if not m:
        return ""
    tail = _clean_text(m.group(1))
    if BAD_REMAINDER.match(tail):
        return ""
    return tail


def parse_any_date_value(v: Any) -> Optional[str]:
    if v is None:
        return None
    try:
        if isinstance(v, (int, float)) or (isinstance(v, str) and v.isdigit()):
            ts = pd.to_datetime(int(v), unit="ms", utc=True).tz_convert(None).date()
            return ts.isoformat()
    except Exception:
        pass
    if isinstance(v, str):
        return _parse_date_maybe(v)
    return None


def extract_label_date_from_task(task: Mapping[str, Any], label_key: str) -> Optional[str]:
    """Return ISO date for a given label, **preferring task due_date first**.
    Order of precedence when the label is present (in title or any CF name):
      1) task['due_date'] (ms epoch) → ISO
      2) If label in title: parse trailing text after the label, else any date in title
      3) If label in CF name(s): CF value (if parseable), else trailing text in CF name, else any date in CF name
    If label is not present anywhere on the task, returns None.
    """
    tname = task.get("name") or ""
    cfs = task.get("custom_fields") or []

    label_in_title = _label_matches(tname, LABEL_PATTERNS[label_key])
    label_in_any_cf = any(_label_matches((cf.get("name") or ""), LABEL_PATTERNS[label_key]) for cf in cfs)

    if not (label_in_title or label_in_any_cf):
        return None

    # 1) Prefer the task due_date when label is present anywhere
    iso_due = parse_any_date_value(task.get("due_date"))
    if iso_due:
        return iso_due

    # 2) Title-based extraction
    if label_in_title:
        tail = extract_trailing_after_label(label_key, tname)
        iso = _parse_date_maybe(tail)
        if iso:
            return iso
        iso = _parse_date_maybe(tname)
        if iso:
            return iso

    # 3) Custom-field-based extraction
    for cf in cfs:
        cf_name = cf.get("name") or ""
        if not _label_matches(cf_name, LABEL_PATTERNS[label_key]):
            continue
        iso = parse_any_date_value(cf.get("value"))
        if iso:
            return iso
        tail = extract_trailing_after_label(label_key, cf_name)
        iso = _parse_date_maybe(tail)
        if iso:
            return iso
        iso = _parse_date_maybe(cf_name)
        if iso:
            return iso

    return None

# -----------------------------
# Domain-specific helpers
# -----------------------------
CODE_PATTERN = re.compile(r"\b([SPZ]\d{5,})\b", re.IGNORECASE)


def extract_all_codes(text: str) -> str:
    if not text:
        return ""
    matches = CODE_PATTERN.findall(text)
    if not matches:
        return ""
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
        # Dates
        record_date = extract_label_date_from_task(t, "record_date")
        meeting_date = extract_label_date_from_task(t, "meeting_date")
        # Codes
        codes = extract_all_codes(" ".join([name or "", desc or ""]))
        rows.append({
            "Task ID": t.get("id"),
            "Task Name": name,
            "URL": t.get("url"),
            "Status": (t.get("status") or {}).get("status"),
            "Assignees": ", ".join([a.get("username") or a.get("email") or a.get("id") for a in t.get("assignees", [])]) if t.get("assignees") else "",
            "Record Date": record_date,
            "Meeting Date": meeting_date,
            "Codes (S/P/Z)": codes,
        })
    df = pd.DataFrame(rows)
    if not df.empty and "Task Name" in df.columns:
        df = df[df["Task Name"].str.strip().str.upper() != "PROJECT LIST TEMPLATE"].copy()
    if "Record Date" in df.columns:
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

with st.spinner("Parsing tasks…"):
    df = tasks_to_dataframe(tasks)

st.dataframe(df, use_container_width=True)

@st.cache_data
def df_to_csv_bytes(dataframe: pd.DataFrame) -> bytes:
    return dataframe.to_csv(index=False).encode("utf-8")

csv_bytes = df_to_csv_bytes(df)
st.download_button("Download CSV", data=csv_bytes, file_name="clickup_tasks.csv", mime="text/csv")

st.caption("Tip: Adjust LABEL_PATTERNS to catch other fuzzy labels (e.g., ICS Job, MC, etc.). Dates are pulled from custom fields, task name trailers, parsed text, or due_date.")
