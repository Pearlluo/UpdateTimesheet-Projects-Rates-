import os
import io
import json
import time
import random
import requests
import pandas as pd
from base64 import b64encode
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from azure.storage.blob import BlobServiceClient


# ================================================================
# CONFIG
# ================================================================
TOKEN_URL  = "https://auth.opms.com.au/api/authenticate/token"
API_BASE   = "https://api.opms.com.au"
TS_URL     = f"{API_BASE}/timesheets/entries"

CLIENT_ID     = os.getenv("OPMS_CLIENT_ID")
CLIENT_SECRET = os.getenv("OPMS_CLIENT_SECRET")

DEFAULT_MODIFIED_SINCE  = "2024-01-01T00:00:00Z"
DEFAULT_PAGE_SIZE       = 25
DEFAULT_SLEEP_SECONDS   = 0.3
DEFAULT_TIMEOUT_SECONDS = 60
DEFAULT_MAX_RETRIES     = 8
DEFAULT_RETRY_STATUS    = {502, 503, 504}
DEFAULT_CHECKPOINT_FILE = Path("../Archived/opms_timesheets_checkpoint.json")

# ── Blob config ──────────────────────────────────────────────────
AZURE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_CONTAINER         = os.getenv("AZURE_BLOB_CONTAINER", "timesheethour")
BLOB_ROSTER             = "data/roster.parquet"
BLOB_TIMESHEET          = "data/timesheet.parquet"
BLOB_MERGED             = "data/merged.parquet"
BLOB_CHECKPOINT         = "config/opms_timesheets_checkpoint.json"


# ================================================================
# VALIDATION
# ================================================================
def validate_config() -> None:
    if not CLIENT_ID:
        raise ValueError("Missing environment variable: OPMS_CLIENT_ID")
    if not CLIENT_SECRET:
        raise ValueError("Missing environment variable: OPMS_CLIENT_SECRET")


def validate_start_date(start_date: Optional[str]) -> None:
    if start_date is None:
        return
    try:
        datetime.strptime(start_date, "%Y-%m-%d")
    except ValueError:
        raise ValueError("start_date must be in YYYY-MM-DD format")


# ================================================================
# AUTH
# ================================================================
def get_access_token() -> str:
    validate_config()
    auth_str = f"{CLIENT_ID}:{CLIENT_SECRET}"
    b64_auth = b64encode(auth_str.encode()).decode()
    headers = {
        "Authorization": f"Basic {b64_auth}",
        "Content-Type":  "application/x-www-form-urlencoded",
        "Accept":        "application/json"
    }
    res = requests.post(
        TOKEN_URL,
        headers=headers,
        data={"grant_type": "client_credentials"},
        timeout=DEFAULT_TIMEOUT_SECONDS
    )
    res.raise_for_status()
    token = res.json().get("access_token")
    if not token:
        raise RuntimeError(f"Token response missing access_token: {res.text[:500]}")
    return token


# ================================================================
# HTTP HELPER WITH RETRY
# ================================================================
def get_with_retry(
    url:             str,
    headers:         dict,
    params:          dict,
    timeout_seconds: int           = DEFAULT_TIMEOUT_SECONDS,
    max_retries:     int           = DEFAULT_MAX_RETRIES,
    retry_status:    Optional[set] = None
) -> requests.Response:
    retry_status = retry_status or DEFAULT_RETRY_STATUS
    last_error: Any = None

    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=timeout_seconds)
            if r.status_code == 401:
                raise RuntimeError("401 Unauthorized")
            if r.status_code in retry_status:
                wait = min(60, 2 ** attempt) + random.uniform(0, 1.5)
                print(f"⚠️  HTTP {r.status_code} (attempt {attempt}/{max_retries}), sleep {wait:.1f}s")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r
        except requests.exceptions.RequestException as e:
            last_error = e
            wait = min(60, 2 ** attempt) + random.uniform(0, 1.5)
            print(f"⚠️  Request error (attempt {attempt}/{max_retries}): {e}, sleep {wait:.1f}s")
            time.sleep(wait)
        except RuntimeError:
            raise

    raise RuntimeError(f"❌ Failed after {max_retries} retries. Last error: {last_error}")


# ================================================================
# CHECKPOINT — local file
# ================================================================
def load_checkpoint(checkpoint_file: Path) -> Dict[str, Any]:
    if checkpoint_file.exists():
        try:
            return json.loads(checkpoint_file.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def save_checkpoint(
    checkpoint_file: Path,
    next_cursor:     Optional[str],
    fetched_count:   int,
    modified_since:  str
) -> None:
    payload = {
        "next_cursor":    next_cursor,
        "fetched_count":  fetched_count,
        "modified_since": modified_since,
        "updated_at":     datetime.now().strftime("%Y/%m/%d %H:%M:%S")
    }
    checkpoint_file.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )


def clear_checkpoint(checkpoint_file: Path) -> None:
    if checkpoint_file.exists():
        checkpoint_file.unlink()


# ================================================================
# CHECKPOINT — blob
# ================================================================
def _blob_client(blob_path: str):
    svc = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    return svc.get_blob_client(container=AZURE_CONTAINER, blob=blob_path)


def load_blob_checkpoint() -> Dict[str, Any]:
    try:
        data = _blob_client(BLOB_CHECKPOINT).download_blob().readall()
        return json.loads(data.decode("utf-8"))
    except Exception:
        return {}


def save_blob_checkpoint(
    next_cursor:    Optional[str],
    fetched_count:  int,
    modified_since: str
) -> None:
    payload = {
        "next_cursor":    next_cursor,
        "fetched_count":  fetched_count,
        "modified_since": modified_since,
        "updated_at":     datetime.now().strftime("%Y/%m/%d %H:%M:%S")
    }
    data = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    _blob_client(BLOB_CHECKPOINT).upload_blob(data, overwrite=True)


# ================================================================
# BLOB — upload DataFrame as Parquet
# ================================================================
def upload_df_to_blob(df: pd.DataFrame, blob_path: str) -> None:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    _blob_client(blob_path).upload_blob(buf, overwrite=True)
    print(f"✅ Uploaded → blob://{AZURE_CONTAINER}/{blob_path}  ({len(df)} rows)")


def download_df_from_blob(blob_path: str) -> Optional[pd.DataFrame]:
    try:
        data = _blob_client(blob_path).download_blob().readall()
        return pd.read_parquet(io.BytesIO(data), engine="pyarrow")
    except Exception as e:
        if "BlobNotFound" in str(e) or "404" in str(e):
            print(f"⚠️  Blob not found: {blob_path}")
            return None
        raise


# ================================================================
# ROSTER — helpers
# ================================================================
def iter_date_windows(start_d: date, end_d: date, window_days: int = 90):
    cur = start_d
    while cur <= end_d:
        win_end = min(cur + timedelta(days=window_days - 1), end_d)
        yield cur, win_end
        cur = win_end + timedelta(days=1)


def chunk_list(lst: List[int], n: int):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


# ================================================================
# ROSTER — fetch all employee IDs
# ================================================================
def get_all_employees(access_token: str) -> List[int]:
    url = f"{API_BASE}/employee/all"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept":        "application/json"
    }
    resp = requests.get(url, headers=headers, timeout=DEFAULT_TIMEOUT_SECONDS)
    resp.raise_for_status()
    employees    = resp.json()
    employee_ids = [e["id"] for e in employees if isinstance(e, dict) and "id" in e]
    print(f"✅ Total employees: {len(employee_ids)}")
    return employee_ids


# ================================================================
# ROSTER — single batch + window call
# ================================================================
def call_roster(
    access_token:       str,
    employee_ids_batch: List[int],
    start_d:            date,
    end_d:              date,
    max_retry:          int = 3
) -> Tuple[Any, int, str]:
    url = f"{API_BASE}/roster"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept":        "application/json",
        "Origin":        "https://webportal.opms.com.au",
        "Referer":       "https://webportal.opms.com.au/"
    }
    params = {
        "employee_ids": ",".join(map(str, employee_ids_batch)),
        "start_date":   start_d.isoformat(),
        "end_date":     end_d.isoformat()
    }
    last_err = ""
    for attempt in range(1, max_retry + 1):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=90)
            if resp.status_code == 200:
                return resp.json(), 200, ""
            if resp.status_code == 401:
                return None, 401, resp.text
            last_err = resp.text
            return None, resp.status_code, resp.text
        except requests.RequestException as e:
            last_err = str(e)
            if attempt < max_retry:
                time.sleep(1.5 * attempt)
            else:
                return None, 0, last_err
    return None, 0, last_err


# ================================================================
# ROSTER — parse API response → flat rows
# ================================================================
def parse_roster_rows(roster_json: Any) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if roster_json is None:
        return rows

    data_list = roster_json if isinstance(roster_json, list) else [roster_json]

    for item in data_list:
        emp        = item.get("employee") or {}
        emp_id     = emp.get("id")
        emp_first  = emp.get("first_name")
        emp_middle = emp.get("middle_name")
        emp_last   = emp.get("last_name")

        for day in item.get("rostered_days") or []:
            day_date = day.get("date")

            pos               = day.get("position") or {}
            day_position_id   = pos.get("id")
            day_position_name = pos.get("name")

            wt             = day.get("work_type") or {}
            work_type_id   = wt.get("id")
            work_type_name = wt.get("name")

            allowances      = day.get("allowances") or []
            allowance_names = "; ".join([a.get("name", "") for a in allowances if a.get("name")])
            allowance_total = sum([(a.get("value") or 0) for a in allowances])

            rras = day.get("resource_request_allocations") or []

            base = {
                "EmployeeID":     emp_id,
                "FirstName":      emp_first,
                "MiddleName":     emp_middle,
                "LastName":       emp_last,
                "Date":           day_date,
                "DayPositionID":  day_position_id,
                "DayPosition":    day_position_name,
                "WorkTypeID":     work_type_id,
                "WorkType":       work_type_name,
                "AllowanceNames": allowance_names,
                "AllowanceTotal": allowance_total,
            }

            if rras:
                for rra in rras:
                    rra_pos = rra.get("position") or {}
                    rr      = rra.get("resource_request") or {}
                    rows.append({
                        **base,
                        "RRA_ID":            rra.get("id"),
                        "RRA_PositionID":    rra_pos.get("id"),
                        "RRA_Position":      rra_pos.get("name"),
                        "Shift":             rra.get("shift"),
                        "ResourceRequestID": rr.get("id"),
                        "Project":           rr.get("project"),
                    })
            else:
                rows.append({
                    **base,
                    "RRA_ID":            None,
                    "RRA_PositionID":    None,
                    "RRA_Position":      None,
                    "Shift":             None,
                    "ResourceRequestID": None,
                    "Project":           None,
                })

    return rows


# ================================================================
# ROSTER — full pull
# ================================================================
def fetch_all_roster_rows(
    start_date:  date,
    end_date:    date,
    window_days: int = 90,
    emp_batch:   int = 50
) -> List[Dict[str, Any]]:
    token        = get_access_token()
    employee_ids = get_all_employees(token)
    all_rows:    List[Dict[str, Any]] = []

    for win_start, win_end in iter_date_windows(start_date, end_date, window_days):
        print(f"\n📅 Window: {win_start} -> {win_end}")

        for batch in chunk_list(employee_ids, emp_batch):
            data, code, err = call_roster(token, batch, win_start, win_end)

            if code == 401:
                print("🔄 Token expired, refreshing token...")
                token = get_access_token()
                data, code, err = call_roster(token, batch, win_start, win_end)

            if code != 200:
                print(f"⚠️  /roster failed: code={code}, batch_first={batch[0]}, window={win_start}->{win_end}")
                print((err or "")[:500])
                continue

            rows = parse_roster_rows(data)
            all_rows.extend(rows)
            print(f"✅ batch ok: {len(batch)} employees, rows_added={len(rows)}")

    return all_rows


# ================================================================
# TIMESHEET — paged fetch with checkpoint
#
# 改动说明（相比原版）:
#   原版在 checkpoint 的 modified_since 与传入值不同时直接忽略 cursor，
#   导致 main.py 每次推进 modified_since 后 cursor 失效，变成全量重拉。
#
#   改动：去掉 modified_since 不一致时忽略 cursor 的逻辑。
#   cursor 只管"从哪页续拉"，modified_since 只管"拉哪个时间范围"，两者独立。
#   只要有 cursor 就续拉，不管 modified_since 是否变了。
# ================================================================
def fetch_all_timesheets(
    modified_since:  str,
    page_size:       int   = DEFAULT_PAGE_SIZE,
    sleep_seconds:   float = DEFAULT_SLEEP_SECONDS,
    checkpoint_file: Path  = DEFAULT_CHECKPOINT_FILE,
    use_checkpoint:  bool  = True
) -> List[Dict[str, Any]]:
    token   = get_access_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept":        "application/json"
    }
    params: Dict[str, Any] = {
        "modified_since": modified_since,
        "page_size":      page_size
    }

    already = 0

    if use_checkpoint:
        checkpoint    = load_checkpoint(checkpoint_file)
        resume_cursor = checkpoint.get("next_cursor")
        already       = checkpoint.get("fetched_count", 0)

        # ── 改动：只要有 cursor 就续拉，不再因 modified_since 不同而丢弃 cursor ──
        if resume_cursor:
            params["after"] = resume_cursor
            print(f"🔁 Resume from cursor (already fetched ≈ {already})")

    all_timesheets: List[Dict[str, Any]] = []
    page = 0

    while True:
        try:
            r = get_with_retry(TS_URL, headers, params)
        except RuntimeError as e:
            if "401 Unauthorized" in str(e):
                print("🔄 Token expired, refreshing token...")
                token = get_access_token()
                headers["Authorization"] = f"Bearer {token}"
                r = get_with_retry(TS_URL, headers, params)
            else:
                raise

        data        = r.json()
        batch       = data.get("timesheets", [])
        all_timesheets.extend(batch)
        page       += 1
        next_cursor = data.get("next_cursor")
        total       = already + len(all_timesheets)

        print(f"✅ Fetched page {page}, timesheets so far: {total}")

        if use_checkpoint:
            save_checkpoint(checkpoint_file, next_cursor, total, modified_since)

        if not next_cursor:
            break

        params["after"] = next_cursor
        time.sleep(sleep_seconds)

    return all_timesheets


# ================================================================
# TIMESHEET — flatten entries → long DataFrame
# ================================================================
def flatten_to_long(
    all_ts:     List[Dict[str, Any]],
    start_date: Optional[str] = None
) -> pd.DataFrame:
    validate_start_date(start_date)

    rows: List[Dict[str, Any]] = []

    for ts in all_ts:
        ts_date = ts.get("date")
        if not ts_date:
            continue
        if start_date and ts_date < start_date:
            continue

        for it in ts.get("entries", []) or []:
            emp      = it.get("employee", {}) or {}
            emp_id   = emp.get("id")
            first    = (emp.get("first_name") or "").strip()
            last     = (emp.get("last_name") or "").strip()
            emp_name = (first + " " + last).strip()
            hours    = it.get("value", 0) or 0

            rows.append({
                "date":          ts_date,
                "employee_id":   emp_id,
                "employee_name": emp_name,
                "hours":         float(hours)
            })

    df = pd.DataFrame(rows)

    if not df.empty:
        df = df.groupby(
            ["date", "employee_id", "employee_name"],
            as_index=False
        )["hours"].sum()

    return df


# ================================================================
# MERGE — timesheet df + roster df
# ================================================================
def merge_timesheet_roster(
    ts_df: pd.DataFrame,
    rs_df: pd.DataFrame
) -> pd.DataFrame:
    ts_df = ts_df.copy()
    rs_df = rs_df.copy()

    ts_df.rename(columns={"date": "Date", "employee_id": "EmployeeID"}, inplace=True)

    ts_df["Date"] = pd.to_datetime(ts_df["Date"])
    rs_df["Date"] = pd.to_datetime(rs_df["Date"])

    drop_cols = [c for c in rs_df.columns if c.lower() in {"employee_name", "hours"}]
    rs_df.drop(columns=drop_cols, errors="ignore", inplace=True)

    merged = ts_df.merge(rs_df, how="left", on=["EmployeeID", "Date"])
    merged.sort_values(["EmployeeID", "Date"], inplace=True)

    return merged


# ================================================================
# SCRIPT ENTRY
# 所有时间参数从 blob: config/pipeline_state.json 读取，不写死
# ================================================================
BLOB_STATE = "config/pipeline_state.json"
INITIAL_MODIFIED_SINCE = "2024-01-01T00:00:00Z"
INITIAL_START_DATE     = date(2024, 1, 1)

if __name__ == "__main__":

    # ── 读 pipeline_state.json ───────────────────────────────────
    try:
        raw   = _blob_client(BLOB_STATE).download_blob().readall()
        state = json.loads(raw.decode("utf-8"))
        print(f"📂 State loaded: {state}")
    except Exception:
        state = {}
        print("🆕 No state found, using defaults")

    roster_last_end   = date.fromisoformat(state.get("roster_last_end",   INITIAL_START_DATE.isoformat()))
    modified_since    = state.get("timesheet_modified_since", INITIAL_MODIFIED_SINCE)
    ts_cursor         = state.get("timesheet_cursor")
    ts_fetched        = state.get("timesheet_fetched_count", 0)

    # ── Phase 1: Roster (增量) ───────────────────────────────────
    start = roster_last_end + timedelta(days=1)
    end   = date.today()

    if start > end:
        print(f"✅ Roster up to date (last_end={roster_last_end}), skipping")
    else:
        print(f"📅 Fetching roster: {start} → {end}")
        new_rows = fetch_all_roster_rows(
            start_date=start,
            end_date=end,
            window_days=90,
            emp_batch=50
        )
        new_df = pd.DataFrame(new_rows).drop_duplicates()

        try:
            existing_raw = _blob_client(BLOB_ROSTER).download_blob().readall()
            existing_rs  = pd.read_parquet(io.BytesIO(existing_raw), engine="pyarrow")
            rs_df = pd.concat([existing_rs, new_df], ignore_index=True).drop_duplicates()
            print(f"✅ Combined roster: {len(rs_df)} rows (was {len(existing_rs)})")
        except Exception:
            rs_df = new_df

        upload_df_to_blob(rs_df, BLOB_ROSTER)
        state["roster_last_end"] = end.isoformat()

    # ── Phase 2: Timesheet (增量，从 state 读时间和 cursor) ──────
    checkpoint = {}
    if ts_cursor:
        checkpoint = {
            "next_cursor":    ts_cursor,
            "fetched_count":  ts_fetched,
            "modified_since": modified_since,
        }
        print(f"🔁 Resuming cursor, already ≈ {ts_fetched}, modified_since={modified_since}")
    else:
        print(f"🆕 Fresh fetch, modified_since={modified_since}")

    tmp_cp = Path("/tmp/opms_timesheets_checkpoint.json")
    tmp_cp.write_text(json.dumps(checkpoint), encoding="utf-8")

    all_ts = fetch_all_timesheets(
        modified_since=modified_since,
        page_size=25,
        checkpoint_file=tmp_cp,
        use_checkpoint=True
    )

    # 读拉完后的 checkpoint，推进 modified_since
    final_cp = {}
    if tmp_cp.exists():
        try:
            final_cp = json.loads(tmp_cp.read_text(encoding="utf-8"))
        except Exception:
            pass

    today_iso = date.today().strftime("%Y-%m-%dT00:00:00Z")
    if not final_cp.get("next_cursor"):
        state["timesheet_cursor"]         = None
        state["timesheet_fetched_count"]  = 0
        state["timesheet_modified_since"] = today_iso   # ← 推进到今天
        print(f"✅ Fetch complete, modified_since → {today_iso}")
    else:
        state["timesheet_cursor"]         = final_cp["next_cursor"]
        state["timesheet_fetched_count"]  = final_cp.get("fetched_count", 0)
        state["timesheet_modified_since"] = modified_since
        print(f"⏸  Incomplete, cursor saved")

    ts_df = flatten_to_long(all_ts, start_date=INITIAL_START_DATE.isoformat())
    print(f"\n✅ Timesheet rows this run: {len(ts_df)}")
    upload_df_to_blob(ts_df, BLOB_TIMESHEET)

    # ── Phase 3: Merge ───────────────────────────────────────────
    rs_df  = pd.read_parquet(io.BytesIO(
        _blob_client(BLOB_ROSTER).download_blob().readall()
    ), engine="pyarrow")
    merged = merge_timesheet_roster(ts_df, rs_df)
    upload_df_to_blob(merged, BLOB_MERGED)
    print(f"\n🎉 DONE — Merged rows: {len(merged)}")

    # ── 写档 ────────────────────────────────────────────────────
    state["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data = json.dumps(state, ensure_ascii=False, indent=2).encode("utf-8")
    _blob_client(BLOB_STATE).upload_blob(data, overwrite=True)
    print(f"💾 State saved → blob://{AZURE_CONTAINER}/{BLOB_STATE}")