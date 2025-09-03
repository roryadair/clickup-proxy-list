import io
import re
import time
from typing import Dict, Any, List, Tuple, Optional

import pandas as pd
import requests
import streamlit as st

API_BASE = "https://api.clickup.com/api/v2"

# ---------- Config: fixed targets ----------
WORKSPACE_NAME = "Fund Solution Workspace"
SPACE_NAME = "ACTIVE Proxy Efforts"

# ---------- Streamlit UI ----------
st.set_page_config(page_title="ACTIVE Proxy Jobs Export", page_icon="📊")
st.title("ACTIVE Proxy Jobs Export")
st.caption(
    "Exports a 6-column Excel from ClickUp → Job Number, Job Name, Broadridge MC, BRD S/P/Z Job Number, Record Date, Meeting Date."
)

# Always read token from Streamlit Secrets
if "CLICKUP_TOKEN" not in st.secrets:
    st.error("Missing CLICKUP_TOKEN in Streamlit Secrets. Please add it in app settings.")
    st.stop()

token = st.secrets["CLICKUP_TOKEN"]

# ---------- HTTP helpers ----------
def auth_headers(tok: str) -> Dict[str, str]:
    return {"Authorization": tok, "Content-Type": "application/json"}

def backoff_sleep(attempt: int) -> None:
    time.sleep(min(10, 2 ** attempt))

def get_json(url: str, headers: Dict[str, str], params: Dict[str, Any] = None) -> Dict[str, Any]:
    attempt = 0
    while True:
        resp = requests.get(url, headers=headers, params=params, timeout=60)
        if resp.status_code == 429:
            attempt += 1
            backoff_sleep(attempt)
            continue
        resp.raise_for_status()
        return resp.json()

# ---------- ClickUp discovery ----------
def get_workspaces(token: str) -> List[Dict[str, Any]]:
    return get_json(f"{API_BASE}/team", auth_headers(token)).get("teams", [])

def get_spaces(team_id: str, token: str) -> List[Dict[str, Any]]:
    return get_json(f"{API_BASE}/team/{team_id}/space", auth_headers(token)).get("spaces", [])

def get_space_folders(space_id: str, token: str) -> List[Dict[str, Any]]:
    return get_json(f"{API_BASE}/space/{space_id}/folder", auth_headers(token)).get("folders", [])

def get_lists_in_folder(folder_id: str, token: str) -> List[Dict[str, Any]]:
    return get_json(f"{API_BASE}/folder/{folder_id}/list", auth_headers(token)).get("lists", [])

# ---------- Task fetch ----------
def fetch_list_tasks(
    token: str,
    list_id: str,
    include_closed: bool = True,
    include_subtasks: bool = True,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    headers = auth_headers(token)
    tasks, page = [], 0
    while True:
        params = {
            "page": page,
            "limit": limit,
            "include_closed": str(include_closed).lower(),
            "subtasks": str(include_subtasks).lower(),
        }
        data = get_json(f"{API_BASE}/list/{list_id}/task", headers, params=params)
        batch = data.get("tasks", [])
        tasks.extend(batch)
        if data.get("last_page") is True or len(batch) < limit:
            break
        page += 1
    return tasks

# ---------- Parsing helpers ----------
# Folder title → Job numbers + Job name
base_re = re.compile(r"^\s*(\d{6})(.*)$")
dash_suffix_re = re.compile(r"-\s*(\d{3})")
comma_full_re = re.compile(r",\s*(\d{6})")
name_after_paren_re = re.compile(r"\([^)]+\)\s*(.*)$")
trailing_paren_number_re = re.compile(r"\(\d+\)\s*$")

def parse_folder_multi(title: str) -> List[Tuple[str, str]]:
    """Return list of (job_number, job_name) rows based on folder title rules."""
    if not title:
        return []
    t = title.strip()
    m = base_re.match(t)
    if not m:
        return [("", t)]
    base_num = m.group(1)
    remainder = m.group(2)

    comma_nums = comma_full_re.findall(remainder)
    prefix = base_num[:3]
    dash_full = [prefix + s for s in dash_suffix_re.findall(remainder)]

    seen, nums = set(), []
    for n in [base_num, *comma_nums, *dash_full]:
        if n not in seen:
            seen.add(n)
            nums.append(n)

    nm_match = name_after_paren_re.search(t)
    job_name = nm_match.group(1).strip() if nm_match else t
    job_name = trailing_paren_number_re.sub("", job_name).strip()
    return [(n, job_name) for n in nums]

# Codes inside folder contents
mc_re = re.compile(r"\b(?:MC\d{4}|MCA\d{3})\b", re.IGNORECASE)
brd_re = re.compile(r"\b([SPZ]\d{5})\b", re.IGNORECASE)

def extract_mc_from_text(text: str) -> str:
    """Return the first MC#### or MCA### code in a given text string, or '' if none."""
    if not text:
        return ""
    m = mc_re.search(text)
    return m.group(0).upper() if m else ""

def find_all_brd(text: str) -> List[str]:
    """Return ALL BRD codes (S#####/P#####/Z#####) in order of appearance, uppercased."""
    if not text:
        return []
    return [m.group(1).upper() for m in brd_re.finditer(text)]

# ---------- Date label parsing (fuzzy, with fallback) ----------
# Word-boundary label matchers; we avoid false hits like "RECORD DATE RANGE"
LABEL_PATTERNS: Dict[str, re.Pattern] = {
    "record_date": re.compile(r"\bRECORD\s*DATE\b", re.IGNORECASE),
    "meeting_date": re.compile(r"\bMEETING\s*DATE\b", re.IGNORECASE),
}

# Phrases that mean the label is not a single date value
# (Removed the buggy `|S|`; added `|TO|`.)
BAD_REMAINDER = re.compile(r"^(RANGE|WINDOW|UNTIL|THRU|THROUGH|TO)\b", re.IGNORECASE)

def _clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def _parse_iso_from_text(s: str) -> Optional[str]:
    if not s:
        return None
    try:
        ts = pd.to_datetime(s, errors="coerce")
        if pd.isna(ts):
            return None
        return ts.date().isoformat()
    except Exception:
        return None

def merge_latest_date(current_iso: str, new_ms) -> str:
    """Keep later date (YYYY-MM-DD) given a ClickUp ms timestamp or None."""
    if not new_ms:
        return current_iso
    try:
        ts = pd.to_datetime(int(new_ms), unit="ms", utc=True).tz_convert(None).date()
        new_iso = ts.isoformat()
    except Exception:
        return current_iso
    if not current_iso:
        return new_iso
    return max(current_iso, new_iso)

def merge_latest_iso(current_iso: str, new_iso: Optional[str]) -> str:
    """Keep the later of two ISO dates (YYYY-MM-DD)."""
    if not new_iso:
        return current_iso
    if not current_iso:
        return new_iso
    return max(current_iso, new_iso)

def label_in_text(label_key: str, text: str) -> bool:
    pat = LABEL_PATTERNS[label_key]
    return bool(pat.search(text or ""))

def extract_trailing_after_label(label_key: str, text: str) -> str:
    """If label appears in text (e.g., 'RECORD DATE: APRIL 17, 2025'), return the trailing part."""
    pat = LABEL_PATTERNS[label_key]
    m = re.search(rf"{pat.pattern}[:\-\s]*(.+)$", text or "", re.IGNORECASE)
    if not m:
        return ""
    tail = _clean_text(m.group(1))
    if BAD_REMAINDER.match(tail):
        return ""
    return tail

def parse_any_date_value(v: Any) -> Optional[str]:
    """Parse ClickUp date-like values to ISO. Supports ms epoch ints and strings like 'Apr 17, 2025'."""
    if v is None:
        return None
    # numeric epoch (assume ms)
    try:
        if isinstance(v, (int, float)) or (isinstance(v, str) and v.isdigit()):
            ms = int(v)
            ts = pd.to_datetime(ms, unit="ms", utc=True).tz_convert(None).date()
            return ts.isoformat()
    except Exception:
        pass
    # string date
    if isinstance(v, str):
        return _parse_iso_from_text(v)
    return None

def scan_folder_metadata(folder_id: str, token: str) -> dict:
    """
    Single-pass scan of a folder to gather:
      - mc_code: first MC####/MCA### found (lists -> tasks -> CF strings)
      - brd_code: ALL S/P/Z + 5 digits found (lists -> tasks -> CF strings), joined by ", "
      - record_date: latest date from tasks labelled like 'RECORD DATE' (prefer due_date, else parse title)
      - meeting_date: latest date from tasks labelled like 'MEETING DATE' (prefer due_date, else parse title)
    """
    meta = {"mc_code": "", "brd_code": "", "record_date": "", "meeting_date": ""}

    lists = get_lists_in_folder(folder_id, token)

    # Helper: append BRD codes preserving first-seen order (no duplicates)
    brd_accum: List[str] = []
    seen_codes = set()

    def add_brd_codes(codes: List[str]):
        nonlocal brd_accum, seen_codes
        for c in codes:
            if c not in seen_codes:
                seen_codes.add(c)
                brd_accum.append(c)

    # Quick pass on list names for codes
    for l in lists:
        name = l.get("name") or ""
        if not meta["mc_code"]:
            mc = extract_mc_from_text(name)
            if mc:
                meta["mc_code"] = mc
        add_brd_codes(find_all_brd(name))

    # Tasks (names + string CFs) for codes and dates
    for l in lists:
        for t in fetch_list_tasks(token, str(l["id"]), include_closed=True, include_subtasks=True, limit=100):
            tname = t.get("name") or ""
            due_ms = t.get("due_date")

            # MC first-hit only
            if not meta["mc_code"]:
                mc = extract_mc_from_text(tname)
                if mc:
                    meta["mc_code"] = mc

            # BRD: collect all occurrences from title
            add_brd_codes(find_all_brd(tname))

            # ---- RECORD DATE: prefer due_date, else parse from title ----
            if label_in_text("record_date", tname):
                if due_ms:
                    meta["record_date"] = merge_latest_date(meta["record_date"], due_ms)
                else:
                    # 1) Try text after the label
                    tail = extract_trailing_after_label("record_date", tname)
                    iso = _parse_iso_from_text(tail)
                    # 2) Fallback: fuzzy-parse anywhere in the title
                    if not iso:
                        iso = _parse_iso_from_text(tname)
                    meta["record_date"] = merge_latest_iso(meta["record_date"], iso)

            # ---- MEETING DATE: prefer due_date, else parse from title ----
            if label_in_text("meeting_date", tname):
                if due_ms:
                    meta["meeting_date"] = merge_latest_date(meta["meeting_date"], due_ms)
                else:
                    # 1) Try text after the label
                    tail = extract_trailing_after_label("meeting_date", tname)
                    iso = _parse_iso_from_text(tail)
                    # 2) Fallback: fuzzy-parse anywhere in the title
                    if not iso:
                        iso = _parse_iso_from_text(tname)
                    meta["meeting_date"] = merge_latest_iso(meta["meeting_date"], iso)

            # String custom fields: still scan for codes only (NOT dates)
            for cf in (t.get("custom_fields") or []):
                val = cf.get("value")
                if isinstance(val, str):
                    if not meta["mc_code"]:
                        mc = extract_mc_from_text(val)
                        if mc:
                            meta["mc_code"] = mc
                    add_brd_codes(find_all_brd(val))

    # Join all BRD codes into a single cell
    meta["brd_code"] = ", ".join(brd_accum)
    return meta

# ---------- Fixed-run button ----------
if not token:
    st.warning("Add your ClickUp token to proceed (paste above or set in Streamlit Secrets).")
    st.stop()

run = st.button("Build & Export", type="primary", use_container_width=True)

try:
    if run:
        with st.spinner("Resolving Workspace and Space…"):
            teams = get_workspaces(token)
            ws = next(
                (t for t in teams if (t.get("name") or "").strip().lower() == WORKSPACE_NAME.lower()),
                None,
            )
            if not ws:
                st.error(f'Workspace "{WORKSPACE_NAME}" not found for this token.')
                st.stop()
            team_id = str(ws["id"])

            spaces = get_spaces(team_id, token)
            space = next(
                (s for s in spaces if (s.get("name") or "").strip().lower() == SPACE_NAME.lower()),
                None,
            )
            if not space:
                st.error(f'Space "{SPACE_NAME}" not found in workspace "{WORKSPACE_NAME}".')
                st.stop()
            space_id = str(space["id"])

        with st.spinner("Scanning folders and building dataset…"):
            folders = get_space_folders(space_id, token)

            rows: List[Dict[str, Any]] = []
            for f in folders:
                folder_id = str(f.get("id"))
                title = f.get("name") or ""

                jobs = parse_folder_multi(title)
                meta = scan_folder_metadata(folder_id, token)

                for job_num, job_name in jobs:
                    rows.append(
                        {
                            "Job Number": job_num,
                            "Job Name": job_name,
                            "Broadridge MC": meta["mc_code"],
                            "BRD S or P Job Number": meta["brd_code"],
                            "Record Date": meta["record_date"],
                            "Meeting Date": meta["meeting_date"],
                            "Folder ID": folder_id,
                            "Folder Title": title,
                        }
                    )

            df = pd.DataFrame(rows)

            # Remove placeholder rows (e.g. PROJECT LIST TEMPLATE)
            df = df[df["Job Name"].str.upper() != "PROJECT LIST TEMPLATE"]

            df = df.sort_values(["Job Number", "Job Name"], na_position="last").reset_index(drop=True)

            # Shape final columns and convert dates to true date objects
            final_cols = [
                "Job Number",
                "Job Name",
                "Broadridge MC",
                "BRD S or P Job Number",
                "Record Date",
                "Meeting Date",
            ]
            out_df = df.reindex(columns=final_cols).copy()
            for col in ["Record Date", "Meeting Date"]:
                out_df[col] = pd.to_datetime(out_df[col], errors="coerce").dt.date
            out_df = out_df.where(pd.notnull(out_df), "")

            # Write Excel to memory
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="openpyxl", date_format="YYYY-MM-DD") as xw:
                out_df.to_excel(xw, index=False, sheet_name="Jobs")
            buf.seek(0)

            st.success(f"Built {len(out_df)} rows from '{WORKSPACE_NAME}' → '{SPACE_NAME}'.")
            st.download_button(
                "Download ACTIVE_Proxy_Jobs.xlsx",
                data=buf.getvalue(),
                file_name="ACTIVE_Proxy_Jobs.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )

            st.divider()
            st.subheader("Preview")
            st.dataframe(out_df.head(50), use_container_width=True)

except requests.HTTPError as e:
    st.error(f"HTTP error: {e.response.status_code} {e.response.text[:300]}")
except Exception as e:
    st.error(f"Unexpected error: {e}")
