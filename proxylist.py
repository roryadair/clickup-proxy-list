import io
import re
import time
from typing import Dict, Any, List, Tuple

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
st.caption("Exports a 6-column Excel from ClickUp → Job Number, Job Name, Broadridge MC, BRD S/P/Z Job Number, Record Date, Meeting Date.")

token_default = st.secrets.get("CLICKUP_TOKEN", "")
token = st.text_input("ClickUp token (pk_…)", value=token_default, type="password")

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
def fetch_list_tasks(token: str, list_id: str, include_closed=True, include_subtasks=True, limit=100) -> List[Dict[str, Any]]:
    headers = auth_headers(token)
    tasks, page = [], 0
    while True:
        params = {
            "page": page, "limit": limit,
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
    if not text:
        return ""
    m = mc_re.search(text)
    return m.group(0).upper() if m else ""

def extract_brd_from_text(text: str) -> str:
    if not text:
        return ""
    m = brd_re.search(text)
    return m.group(1).upper() if m else ""

def normalize_title(s: str) -> str:
    return (s or "").strip().upper()

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

def scan_folder_metadata(folder_id: str, token: str) -> dict:
    """
    Single-pass scan of a folder to gather:
      - mc_code: first MC####/MCA### found (lists -> tasks -> CF strings)
      - brd_code: first S/P/Z + 5 digits found (lists -> tasks -> CF strings)
      - record_date: latest due_date from tasks named 'RECORD DATE'
      - meeting_date: latest due_date from tasks named 'MEETING DATE'
    """
    meta = {"mc_code": "", "brd_code": "", "record_date": "", "meeting_date": ""}

    lists = get_lists_in_folder(folder_id, token)

    # Fast pass on list names (codes)
    for l in lists:
        if not meta["mc_code"]:
            mc = extract_mc_from_text(l.get("name"))
            if mc:
                meta["mc_code"] = mc
        if not meta["brd_code"]:
            brd = extract_brd_from_text(l.get("name"))
            if brd:
                meta["brd_code"] = brd
        if meta["mc_code"] and meta["brd_code"]:
            break

    # Tasks (codes + dates)
    for l in lists:
        for t in fetch_list_tasks(token, str(l["id"]), include_closed=True, include_subtasks=True, limit=100):
            if not meta["mc_code"]:
                mc = extract_mc_from_text(t.get("name"))
                if mc:
                    meta["mc_code"] = mc
            if not meta["brd_code"]:
                brd = extract_brd_from_text(t.get("name"))
                if brd:
                    meta["brd_code"] = brd

            title_norm = normalize_title(t.get("name", ""))
            if title_norm == "RECORD DATE":
                meta["record_date"] = merge_latest_date(meta["record_date"], t.get("due_date"))
            elif title_norm == "MEETING DATE":
                meta["meeting_date"] = merge_latest_date(meta["meeting_date"], t.get("due_date"))

            if (not meta["mc_code"]) or (not meta["brd_code"]):
                for cf in (t.get("custom_fields") or []):
                    val = cf.get("value")
                    if isinstance(val, str):
                        if not meta["mc_code"]:
                            mc = extract_mc_from_text(val)
                            if mc:
                                meta["mc_code"] = mc
                        if not meta["brd_code"]:
                            brd = extract_brd_from_text(val)
                            if brd:
                                meta["brd_code"] = brd

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
            ws = next((t for t in teams if (t.get("name") or "").strip().lower() == WORKSPACE_NAME.lower()), None)
            if not ws:
                st.error(f'Workspace "{WORKSPACE_NAME}" not found for this token.')
                st.stop()
            team_id = str(ws["id"])

            spaces = get_spaces(team_id, token)
            space = next((s for s in spaces if (s.get("name") or "").strip().lower() == SPACE_NAME.lower()), None)
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
                    rows.append({
                        "Job Number": job_num,
                        "Job Name": job_name,
                        "Broadridge MC": meta["mc_code"],
                        "BRD S or P Job Number": meta["brd_code"],
                        "Record Date": meta["record_date"],
                        "Meeting Date": meta["meeting_date"],
                        "Folder ID": folder_id,
                        "Folder Title": title,
                    })

            df = pd.DataFrame(rows).sort_values(["Job Number", "Job Name"], na_position="last").reset_index(drop=True)

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
