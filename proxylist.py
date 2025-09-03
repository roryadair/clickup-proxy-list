import io
import re
import time
from typing import Dict, Any, List, Tuple, Optional

import pandas as pd
import requests
import streamlit as st

API_BASE = "https://api.clickup.com/api/v2"
USER_TZ = "America/Los_Angeles"  # Convert ClickUp UTC ms → this TZ → date

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

# ---------- Label detection (fuzzy but safe) ----------
# Accept exact labels or titles that START with the label (e.g., "RECORD DATE: ..."),
# while rejecting range-like titles ("RANGE", "WINDOW", "TO", etc.).
RANGE_BLOCK = re.compile(r"^(?:[:\-–—\s]*)(RANGE|WINDOW|THRU|THROUGH|TO|BETWEEN|FROM)\b", re.IGNORECASE)
RECORD_START = re.compile(r"^\s*RECORD\s*DATE\b(.*)$", re.IGNORECASE)
MEETING_START = re.compile(r"^\s*MEETING\s*DATE\b(.*)$", re.IGNORECASE)

def _is_single_label_task(text: str, kind: str) -> bool:
    if not text:
        return False
    t = (text or "").strip()
    up = t.upper()

    if kind == "record":
        if up == "RECORD DATE":
            return True
        m = RECORD_START.match(t)
    else:
        if up == "MEETING DATE":
            return True
        m = MEETING_START.match(t)

    if not m:
        return False
    tail = (m.group(1) or "").strip()
    tail = re.sub(r"^[\s:–—-]+", "", tail)
    return not RANGE_BLOCK.match(tail)

# --- Exact label matchers ---
EXACT_RECORD = re.compile(r"^\s*RECORD\s*DATE\s*$", re.IGNORECASE)
EXACT_MEETING = re.compile(r"^\s*MEETING\s*DATE\s*$", re.IGNORECASE)

def is_exact_label(title: str, kind: str) -> bool:
    if not title:
        return False
    return bool((EXACT_RECORD if kind == "record" else EXACT_MEETING).match(title))

# --- Strict fuzzy matcher (keeps your existing logic & blockers) ---
BLOCKERS_ANY = re.compile(r"\b(RANGE|WINDOW|THRU|THROUGH|TO|BETWEEN|FROM)\b", re.IGNORECASE)
BAD_TAIL_WORDS = re.compile(
    r"\b(FILE|FILES|DOCS?|PDF|UPLOAD|LINK|DRAFT|CHECKLIST|NOTE|NOTES|TEMPLATE|PACKAGE|PKG|MATERIALS?|SUBMISSION|REPORT|STATUS|EMAIL)\b",
    re.IGNORECASE,
)
RECORD_ANY = re.compile(r"\bRECORD\s*DATE\b", re.IGNORECASE)
MEETING_ANY = re.compile(r"\bMEETING\s*DATE\b", re.IGNORECASE)
_SEP_STRIP = re.compile(r"^[\s:–—-]+")

def is_label_task_strict(title: str, kind: str) -> bool:
    """
    Accept ONLY:
      - exact 'RECORD DATE' / 'MEETING DATE', OR
      - label anywhere, followed by separators and a date-ish or placeholder tail.
    Reject range/window/admin-helper titles.
    """
    if not title or BLOCKERS_ANY.search(title):
        return False
    if is_exact_label(title, kind):
        return True

    pat = RECORD_ANY if kind == "record" else MEETING_ANY
    m = pat.search(title)
    if not m:
        return False

    tail = _SEP_STRIP.sub("", (title[m.end():] or "")).strip()
    if tail == "":
        return True  # effectively exact after trailing whitespace/separators

    if BAD_TAIL_WORDS.search(tail):
        return False

    ts = pd.to_datetime(tail, errors="coerce")
    if not pd.isna(ts):
        return True

    if re.fullmatch(r"(TBD|TBA|N/?A|ASAP)", tail, re.IGNORECASE):
        return True

    return False


# --- Stricter label matcher to ignore helper/admin tasks like "Record Date Date File" ---

# Words that mean this isn't a single-day date task
BLOCKERS_ANY = re.compile(r"\b(RANGE|WINDOW|THRU|THROUGH|TO|BETWEEN|FROM)\b", re.IGNORECASE)

# Tails that clearly indicate files/admin (not a date)
BAD_TAIL_WORDS = re.compile(
    r"\b(FILE|FILES|DOCS?|PDF|UPLOAD|LINK|DRAFT|CHECKLIST|NOTE|NOTES|TEMPLATE|PACKAGE|PKG|MATERIALS?|SUBMISSION|REPORT|STATUS|EMAIL)\b",
    re.IGNORECASE,
)

RECORD_ANY = re.compile(r"\bRECORD\s*DATE\b", re.IGNORECASE)
MEETING_ANY = re.compile(r"\bMEETING\s*DATE\b", re.IGNORECASE)

# Strip leading separators right after the label (":", "-", "—", whitespace)
_SEP_STRIP = re.compile(r"^[\s:–—-]+")

def is_label_task_strict(title: str, kind: str) -> bool:
    """
    Accept ONLY:
      - exact 'RECORD DATE' / 'MEETING DATE'
      - OR label followed by separators and a date-ish tail
    Reject if the title/tail looks like a range/window or a file/admin helper.
    """
    if not title:
        return False

    if BLOCKERS_ANY.search(title):
        return False

    pat = RECORD_ANY if kind == "record" else MEETING_ANY
    m = pat.search(title)
    if not m:
        return False

    tail = _SEP_STRIP.sub("", (title[m.end():] or "")).strip()

    # Exact label (no tail) → OK
    if tail == "":
        return True

    # Tails that look like artifacts/admin → reject
    if BAD_TAIL_WORDS.search(tail):
        return False

    # If tail parses as a date, also OK (we still only use due_date — this just validates the label)
    ts = pd.to_datetime(tail, errors="coerce")
    if not pd.isna(ts):
        return True

    # Allow common placeholders
    if re.fullmatch(r"(TBD|TBA|N/?A|ASAP)", tail, re.IGNORECASE):
        return True

    # Anything else (e.g., "Date File") → reject
    return False


# ---------- Date helpers ----------
def iso_from_ms(ms: Any) -> Optional[str]:
    try:
        return pd.to_datetime(int(ms), unit="ms", utc=True).tz_convert(USER_TZ).date().isoformat()
    except Exception:
        return None

def pick_best_iso(candidates: List[Tuple[str, bool]]) -> str:
    """
    candidates: list of (iso_date_str, is_open_status).
    Strategy:
      1) Prefer open tasks; else all tasks.
      2) Among chosen set: earliest date >= today; if none, latest date < today.
    """
    if not candidates:
        return ""

    today = pd.Timestamp.now(tz=USER_TZ).date()

    def pick(pool: List[str]) -> str:
        if not pool:
            return ""
        dti = pd.to_datetime(pool, errors="coerce")
        # Convert each timestamp to a Python date; skip NaT
        dates = sorted([ts.date() for ts in dti if not pd.isna(ts)])
        if not dates:
            return ""
        future = [d for d in dates if d >= today]
        chosen = min(future) if future else max(dates)
        return chosen.isoformat()

    open_pool = [iso for iso, is_open in candidates if is_open]
    chosen = pick(open_pool)
    if chosen:
        return chosen

    return pick([iso for iso, _ in candidates])

def status_is_open(t: Dict[str, Any]) -> bool:
    stobj = t.get("status") or {}
    # ClickUp uses status.type in {"open","closed"}; default to True if missing
    return (stobj.get("type") or "open").lower() != "closed"

# ---------- Scan a folder for codes & dates ----------
def scan_folder_metadata(folder_id: str, token: str) -> dict:
    """
    Single-pass scan of a folder to gather:
      - mc_code: first MC####/MCA### found (lists -> tasks -> CF strings)
      - brd_code: ALL S/P/Z + 5 digits found (lists -> tasks -> CF strings), joined by ", "
      - record_date / meeting_date: chosen from due_date of label tasks
        * PRIORITY: exact 'RECORD DATE'/'MEETING DATE' tasks first; if none, use strict fuzzy
      We only use due_date; no title/CF date parsing.
    """
    meta = {"mc_code": "", "brd_code": "", "record_date": "", "meeting_date": ""}

    lists = get_lists_in_folder(folder_id, token)

    # Helper: append codes preserving first-seen order (no duplicates)
    brd_accum: List[str] = []
    seen_codes = set()

    def add_brd_codes(codes: List[str]):
        nonlocal brd_accum, seen_codes
        for c in codes:
            if c not in seen_codes:
                seen_codes.add(c)
                brd_accum.append(c)

    # Quick pass on list names (codes only)
    for l in lists:
        lname = l.get("name") or ""
        if not meta["mc_code"]:
            mc = extract_mc_from_text(lname)
            if mc:
                meta["mc_code"] = mc
        add_brd_codes(find_all_brd(lname))

    # Collect candidates (split into EXACT vs FUZZY buckets)
    rec_exact:   List[Tuple[str, bool]] = []
    rec_fuzzy:   List[Tuple[str, bool]] = []
    mtg_exact:   List[Tuple[str, bool]] = []
    mtg_fuzzy:   List[Tuple[str, bool]] = []

    for l in lists:
        for t in fetch_list_tasks(token, str(l["id"]), include_closed=True, include_subtasks=True, limit=100):
            tname = t.get("name") or ""
            due_ms = t.get("due_date")

            # Codes
            add_brd_codes(find_all_brd(tname))
            for cf in (t.get("custom_fields") or []):
                val = cf.get("value")
                if isinstance(val, str):
                    if not meta["mc_code"]:
                        mc = extract_mc_from_text(val)
                        if mc:
                            meta["mc_code"] = mc
                    add_brd_codes(find_all_brd(val))

            # Dates (ONLY due_date)
            if not due_ms:
                continue
            iso = iso_from_ms(due_ms)
            if not iso:
                continue
            is_open = status_is_open(t)

            # RECORD
            if is_exact_label(tname, "record"):
                rec_exact.append((iso, is_open))
            elif is_label_task_strict(tname, "record"):
                rec_fuzzy.append((iso, is_open))

            # MEETING
            if is_exact_label(tname, "meeting"):
                mtg_exact.append((iso, is_open))
            elif is_label_task_strict(tname, "meeting"):
                mtg_fuzzy.append((iso, is_open))

    # Pick exact first; only if none exist do we use fuzzy
    meta["record_date"]  = pick_best_iso(rec_exact) or pick_best_iso(rec_fuzzy)
    meta["meeting_date"] = pick_best_iso(mtg_exact) or pick_best_iso(mtg_fuzzy)

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
