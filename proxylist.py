import io
import re
import time
from typing import Dict, Any, List, Tuple, Optional

import pandas as pd
import requests
import streamlit as st

# PDF export
try:
    from fpdf import FPDF
    HAS_FPDF = True
except Exception:
    HAS_FPDF = False

API_BASE = "https://api.clickup.com/api/v2"
USER_TZ = "America/Los_Angeles"  # Convert ClickUp UTC ms → this TZ → date

# ---------- Config: fixed targets ----------
WORKSPACE_NAME = "Fund Solution Workspace"
SPACE_NAME = "ACTIVE Proxy Efforts"

# ---------- Streamlit UI ----------
st.set_page_config(page_title="ACTIVE Proxy Jobs Export", page_icon="📊")
st.title("ACTIVE Proxy Jobs Export")
st.caption(
    "Exports Excel/PDF from ClickUp → Job Number, Job Name, Broadridge MC, "
    "BRD S/P/Z Job Number, Record Date, Meeting Date, Adjournment Date."
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
def fetch_list_tasks(token: str, list_id: str,
    include_closed: bool = True, include_subtasks: bool = True, limit: int = 100) -> List[Dict[str, Any]]:
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
base_re = re.compile(r"^\s*(\d{6})(.*)$")
dash_suffix_re = re.compile(r"-\s*(\d{3})")
comma_full_re = re.compile(r",\s*(\d{6})")
name_after_paren_re = re.compile(r"\([^)]+\)\s*(.*)$")
trailing_paren_number_re = re.compile(r"\(\d+\)\s*$")

def parse_folder_multi(title: str) -> List[Tuple[str, str]]:
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

# ---------- Code detection ----------
mc_re = re.compile(r"\b(?:MC\s?\d{4}|MCA\d{3})\b", re.IGNORECASE)
brd_re = re.compile(r"\b([SPZ]\d{5})\b", re.IGNORECASE)

def extract_mc_from_text(text: str) -> str:
    if not text: return ""
    m = mc_re.search(text)
    return m.group(0).upper() if m else ""

def find_all_brd(text: str) -> List[str]:
    if not text: return []
    return [m.group(1).upper() for m in brd_re.finditer(text)]

def cf_value_to_text(val: Any) -> str:
    if val is None: return ""
    if isinstance(val, str): return val
    if isinstance(val, (int, float)): return str(val)
    if isinstance(val, dict):
        for k in ("label","name","value","title","text"):
            v = val.get(k)
            if isinstance(v,str) and v.strip(): return v
        return " ".join(str(v) for v in val.values() if isinstance(v,(str,int,float)) and str(v).strip())
    if isinstance(val, list):
        parts=[]
        for item in val:
            if isinstance(item,(str,int,float)): parts.append(str(item))
            elif isinstance(item,dict):
                for k in ("label","name","value","title","text"):
                    v=item.get(k)
                    if isinstance(v,str) and v.strip():
                        parts.append(v); break
        return " ".join(parts)
    return str(val)

# ---------- Label detection ----------
EXACT_RECORD = re.compile(r"^\s*RECORD\s*DATE\s*$", re.IGNORECASE)
EXACT_MEETING = re.compile(r"^\s*MEETING\s*DATE\s*$", re.IGNORECASE)
ADJ_EXACT   = re.compile(r"^\s*ADJOURN(?:MENT)?\s*DATE\s*$", re.IGNORECASE)
ADJ_WORD    = re.compile(r"\bADJOURN\w*\b", re.IGNORECASE)
MEETING_WORD= re.compile(r"\bMEETING\w*\b", re.IGNORECASE)

def is_exact_label(title: str, kind: str) -> bool:
    if not title: return False
    return bool((EXACT_RECORD if kind=="record" else EXACT_MEETING).match(title))

def is_adjourn_label(title: str) -> bool:
    if not title: return False
    if ADJ_EXACT.match(title): return True
    return bool(ADJ_WORD.search(title) and MEETING_WORD.search(title))

# ---------- Date helpers ----------
def iso_from_ms(ms: Any) -> Optional[str]:
    try:
        return pd.to_datetime(int(ms), unit="ms", utc=True).tz_convert(USER_TZ).date().isoformat()
    except Exception: return None

def pick_best_iso(candidates: List[Tuple[str, bool]]) -> str:
    if not candidates: return ""
    today = pd.Timestamp.now(tz=USER_TZ).date()
    def pick(pool: List[str]) -> str:
        if not pool: return ""
        dti = pd.to_datetime(pool, errors="coerce")
        dates = sorted([ts.date() for ts in dti if not pd.isna(ts)])
        if not dates: return ""
        future = [d for d in dates if d >= today]
        chosen = min(future) if future else max(dates)
        return chosen.isoformat()
    open_pool = [iso for iso,is_open in candidates if is_open]
    chosen = pick(open_pool)
    if chosen: return chosen
    return pick([iso for iso,_ in candidates])

def status_is_open(t: Dict[str, Any]) -> bool:
    stobj = t.get("status") or {}
    return (stobj.get("type") or "open").lower() != "closed"

# ---------- Scan a folder ----------
def scan_folder_metadata(folder_id: str, token: str) -> dict:
    meta = {"mc_code": "", "brd_code": "", "record_date": "", "meeting_date": "", "adjournment_date": ""}
    lists = get_lists_in_folder(folder_id, token)

    # accumulate BRD
    brd_accum, seen_codes = [], set()
    def add_brd_codes(codes: List[str]):
        for c in codes:
            if c not in seen_codes:
                seen_codes.add(c); brd_accum.append(c)

    rec_exact, mtg_exact, adj_cands = [], [], []

    for l in lists:
        lname = l.get("name") or ""
        if not meta["mc_code"]:
            mc = extract_mc_from_text(lname)
            if mc: meta["mc_code"] = mc
        add_brd_codes(find_all_brd(lname))

    for l in lists:
        for t in fetch_list_tasks(token,str(l["id"]),include_closed=True,include_subtasks=True,limit=100):
            tname = t.get("name") or ""
            due_ms = t.get("due_date")

            if not meta["mc_code"]:
                mc = extract_mc_from_text(tname)
                if mc: meta["mc_code"] = mc
            add_brd_codes(find_all_brd(tname))

            for cf in (t.get("custom_fields") or []):
                text = cf_value_to_text(cf.get("value"))
                if text:
                    if not meta["mc_code"]:
                        mc = extract_mc_from_text(text)
                        if mc: meta["mc_code"] = mc
                    add_brd_codes(find_all_brd(text))

            if not due_ms: continue
            iso = iso_from_ms(due_ms)
            if not iso: continue
            is_open = status_is_open(t)

            if is_exact_label(tname,"record"): rec_exact.append((iso,is_open))
            if is_exact_label(tname,"meeting"): mtg_exact.append((iso,is_open))
            if is_adjourn_label(tname): adj_cands.append((iso,is_open))

    meta["record_date"]      = pick_best_iso(rec_exact)
    meta["meeting_date"]     = pick_best_iso(mtg_exact)
    meta["adjournment_date"] = pick_best_iso(adj_cands)
    meta["brd_code"] = ", ".join(brd_accum)
    return meta

# ---------- Run ----------
run = st.button("Build & Export", type="primary", use_container_width=True)

try:
    if run:
        with st.spinner("Resolving Workspace and Space…"):
            teams = get_workspaces(token)
            ws = next((t for t in teams if (t.get("name") or "").strip().lower() == WORKSPACE_NAME.lower()),None)
            if not ws:
                st.error(f'Workspace "{WORKSPACE_NAME}" not found.')
                st.stop()
            team_id = str(ws["id"])
            spaces = get_spaces(team_id, token)
            space = next((s for s in spaces if (s.get("name") or "").strip().lower() == SPACE_NAME.lower()),None)
            if not space:
                st.error(f'Space "{SPACE_NAME}" not found.')
                st.stop()
            space_id = str(space["id"])

        with st.spinner("Scanning folders and building dataset…"):
            folders = get_space_folders(space_id, token)
            rows=[]
            for f in folders:
                folder_id = str(f.get("id")); title=f.get("name") or ""
                jobs=parse_folder_multi(title)
                meta=scan_folder_metadata(folder_id,token)
                for job_num,job_name in jobs:
                    rows.append({
                        "Job Number": job_num,
                        "Job Name": job_name,
                        "Broadridge MC": meta["mc_code"],
                        "BRD S or P Job Number": meta["brd_code"],
                        "Record Date": meta["record_date"],
                        "Meeting Date": meta["meeting_date"],
                        "Adjournment Date": meta["adjournment_date"],
                        "Folder ID": folder_id,
                        "Folder Title": title,
                    })
            df=pd.DataFrame(rows)
            df=df[df["Job Name"].str.upper()!="PROJECT LIST TEMPLATE"]
            df=df.sort_values(["Job Number","Job Name"],na_position="last").reset_index(drop=True)

            final_cols=["Job Number","Job Name","Broadridge MC","BRD S or P Job Number","Record Date","Meeting Date","Adjournment Date"]
            out_df=df.reindex(columns=final_cols).copy()

            # Convert to datetime.date (NaT if blank)
            for col in ["Record Date","Meeting Date","Adjournment Date"]:
                out_df[col]=pd.to_datetime(out_df[col],errors="coerce").dt.date

            # Build sorted views
            by_name = out_df.sort_values(["Job Name"], na_position="last")
            today = pd.Timestamp.now(tz=USER_TZ).date()

            # Add helper columns
            df_meeting = out_df.copy()
            df_meeting["MeetingDate_tmp"] = pd.to_datetime(df_meeting["Meeting Date"], errors="coerce")
            df_meeting["is_future"] = df_meeting["MeetingDate_tmp"].dt.date >= today
            
            # Custom sort:
            #   1. Future dates first (True=1, False=0, so we invert with ascending=False)
            #   2. Within future → ascending by date
            #   3. Within past → descending by date (nearest past first)
            #   4. NaT at bottom
            future = df_meeting[df_meeting["is_future"]].sort_values("MeetingDate_tmp", ascending=True)
            past   = df_meeting[~df_meeting["is_future"] & df_meeting["MeetingDate_tmp"].notna()].sort_values("MeetingDate_tmp", ascending=False)
            blanks = df_meeting[df_meeting["MeetingDate_tmp"].isna()]

            by_meeting = out_df.sort_values(
                ["Meeting Date"], ascending=False, na_position="last"
            )

            # Replace NaT with blanks for export
            excel_df_name = by_name.fillna("")
            excel_df_meeting = by_meeting.fillna("")

            buf=io.BytesIO()
            with pd.ExcelWriter(buf,engine="openpyxl",date_format="YYYY-MM-DD") as xw:
                excel_df_name.to_excel(xw,index=False,sheet_name="Jobs_by_Name")
                excel_df_meeting.to_excel(xw,index=False,sheet_name="Jobs_by_MeetingDate")
            buf.seek(0)

            st.success(f"Built {len(out_df)} rows from '{WORKSPACE_NAME}' → '{SPACE_NAME}'.")
            st.download_button("Download ACTIVE_Proxy_Jobs.xlsx",
                data=buf.getvalue(),
                file_name="ACTIVE_Proxy_Jobs.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True)

            if HAS_FPDF:
                pdf_cols = ["Job Number","Job Name","Broadridge MC","BRD S or P Job Number",
                            "Record Date","Meeting Date","Adjournment Date"]
            
                pdf = FPDF(orientation="L", unit="mm", format="A4")
                pdf.add_page()
            
                # Scale column widths to fit the full page width
                page_width = pdf.w - 2 * pdf.l_margin
                base_widths = [30, 60, 30, 40, 30, 30, 40]  # your original proportions
                scale = page_width / sum(base_widths)
                col_widths = [w * scale for w in base_widths]
            
                # Header
                pdf.set_font("Arial", "B", 9)
                for i, col in enumerate(pdf_cols):
                    pdf.cell(col_widths[i], 8, col, 1, 0, "C")
                pdf.ln()
            
                # Rows
                # Rows (sorted by Meeting Date newest → oldest)
                pdf.set_font("Arial", "", 8)
                for _, row in by_meeting.fillna("").iterrows():
                    for i, col in enumerate(pdf_cols):
                        val = str(row[col]) if row[col] else ""
                        pdf.cell(col_widths[i], 6, val, 1, 0, "C")
                    pdf.ln()

                pdf_buf = io.BytesIO(pdf.output(dest="S"))
                st.download_button(
                    "Download ACTIVE_Proxy_Jobs.pdf",
                    data=pdf_buf.getvalue(),
                    file_name="ACTIVE_Proxy_Jobs.pdf",
                    mime="application/pdf",
                    use_container_width=True,
                )

            st.dataframe(out_df.head(50),use_container_width=True)

except Exception as e:
    st.error(f"Unexpected error: {e}")
