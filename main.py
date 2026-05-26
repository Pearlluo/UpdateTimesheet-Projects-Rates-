"""
main.py
=======
Main entry point — runs all pipeline phases in sequence, storing all data in Azure Blob.

State management:
  All checkpoints are stored in blob: config/pipeline_state.json
  State is loaded at startup and saved after each run.

pipeline_state.json structure:
  {
    "roster_last_end":           "2026-05-22",
    "timesheet_cursor":          null,
    "timesheet_fetched_count":   0,
    "timesheet_modified_since":  "2026-05-22T00:00:00Z",
    "updated_at":                "2026-05-22 14:29:31 AWST"
  }

Incremental strategy:
  Phase 1: Read roster_last_end, fetch from that date +1 day to today, update on completion
  Phase 2: Resume from cursor if present; otherwise use modified_since for incremental fetch.
           After a full fetch, advance modified_since to today (Perth midnight) so next run only pulls changes.
  Phase 3: Only reprocess (EmployeeID, Date) keys added in this run
  Phase 4/5: Incremental logic handled internally by each sub-module

Usage:
  python main.py
  python main.py --phase roster / timesheet / merge / resolve / rates
  python main.py --upload-map path.csv
"""

import io
import os
import sys
import json
import argparse
import importlib.util
import pandas as pd
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, Dict, Any

from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

load_dotenv()


# ================================================================
# PERTH TIME  (AWST = UTC+8, no daylight saving)
# ================================================================
PERTH_TZ = timezone(timedelta(hours=8))

def perth_today() -> date:
    return datetime.now(tz=PERTH_TZ).date()

def perth_now_str() -> str:
    return datetime.now(tz=PERTH_TZ).strftime("%Y-%m-%d %H:%M:%S AWST")

def perth_today_iso() -> str:
    """Perth today at midnight, used as OPMS modified_since parameter."""
    return perth_today().strftime("%Y-%m-%dT00:00:00Z")


# ================================================================
# BLOB CONFIG
# ================================================================
AZURE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_CONTAINER         = os.getenv("AZURE_BLOB_CONTAINER", "timesheethour")

BLOB_ROSTER      = "data/roster.parquet"
BLOB_TIMESHEET   = "data/timesheet.parquet"
BLOB_MERGED      = "data/merged.parquet"
BLOB_PROJECT_MAP = "config/Project_Client_Map.csv"
BLOB_STATE       = "config/pipeline_state.json"

# Initial defaults used only on first run
INITIAL_START_DATE     = date(2024, 1, 1)
INITIAL_MODIFIED_SINCE = "2024-01-01T00:00:00Z"


# ================================================================
# BLOB HELPERS
# ================================================================
def _blob_client(blob_path: str):
    svc = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    return svc.get_blob_client(container=AZURE_CONTAINER, blob=blob_path)

def upload_df(df: pd.DataFrame, blob_path: str) -> None:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    _blob_client(blob_path).upload_blob(buf, overwrite=True)
    print(f"✅ Uploaded → blob://{AZURE_CONTAINER}/{blob_path}  ({len(df)} rows)")

def download_df(blob_path: str) -> Optional[pd.DataFrame]:
    try:
        data = _blob_client(blob_path).download_blob().readall()
        return pd.read_parquet(io.BytesIO(data), engine="pyarrow")
    except Exception as e:
        if "BlobNotFound" in str(e) or "404" in str(e):
            print(f"⚠️  Blob not found: {blob_path}")
            return None
        raise

def upload_bytes(data: bytes, blob_path: str) -> None:
    _blob_client(blob_path).upload_blob(data, overwrite=True)

def download_bytes(blob_path: str) -> Optional[bytes]:
    try:
        return _blob_client(blob_path).download_blob().readall()
    except Exception as e:
        if "BlobNotFound" in str(e) or "404" in str(e):
            return None
        raise

def upload_csv_file(local_path: str, blob_path: str) -> None:
    with open(local_path, "rb") as f:
        _blob_client(blob_path).upload_blob(f, overwrite=True)
    print(f"✅ Uploaded CSV → blob://{AZURE_CONTAINER}/{blob_path}")


# ================================================================
# STATE — load / save
# ================================================================
def load_state() -> Dict[str, Any]:
    data = download_bytes(BLOB_STATE)
    if data:
        try:
            state = json.loads(data.decode("utf-8"))
            print(f"📂 State loaded:")
            print(f"   roster_last_end          = {state.get('roster_last_end')}")
            print(f"   timesheet_modified_since = {state.get('timesheet_modified_since')}")
            print(f"   timesheet_cursor         = {'resuming' if state.get('timesheet_cursor') else 'fresh run'}")
            print(f"   updated_at               = {state.get('updated_at')}")
            return state
        except Exception as e:
            print(f"⚠️  Failed to parse state JSON: {e}")

    print("🆕 No state file found, initialising defaults")
    return {
        "roster_last_end":           INITIAL_START_DATE.isoformat(),
        "timesheet_cursor":          None,
        "timesheet_fetched_count":   0,
        "timesheet_modified_since":  INITIAL_MODIFIED_SINCE,
        "updated_at":                None,
    }

def save_state(state: Dict[str, Any]) -> None:
    state["updated_at"] = perth_now_str()
    data = json.dumps(state, ensure_ascii=False, indent=2).encode("utf-8")
    upload_bytes(data, BLOB_STATE)
    print(f"💾 State saved → blob://{AZURE_CONTAINER}/{BLOB_STATE}")
    print(f"   roster_last_end          = {state.get('roster_last_end')}")
    print(f"   timesheet_modified_since = {state.get('timesheet_modified_since')}")
    print(f"   timesheet_cursor         = {'present' if state.get('timesheet_cursor') else 'none'}")


# ================================================================
# MODULE LOADER
# ================================================================
def _load_rtc():
    if "Roster_timesheet_com" in sys.modules:
        return sys.modules["Roster_timesheet_com"]
    spec = importlib.util.spec_from_file_location(
        "Roster_timesheet_com",
        os.path.join(os.path.dirname(__file__), "Roster+timesheet+com.py")
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["Roster_timesheet_com"] = mod
    spec.loader.exec_module(mod)
    return mod

def _load_mod(name: str, filename: str):
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(
        name, os.path.join(os.path.dirname(__file__), filename)
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


# ================================================================
# PHASE 1: ROSTER  (incremental, by date)
# ================================================================
def phase_roster(state: Dict[str, Any]) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("PHASE 1: Fetching Roster from OPMS")
    print("=" * 60)

    rtc = _load_rtc()
    roster_last_end = date.fromisoformat(state["roster_last_end"])
    start = roster_last_end + timedelta(days=1)
    end   = perth_today()

    if start > end:
        print(f"✅ Roster up to date (last_end={roster_last_end}), skipping")
        df = download_df(BLOB_ROSTER)
        return df if df is not None else pd.DataFrame()

    print(f"📅 Fetching roster: {start} → {end}")
    new_rows = rtc.fetch_all_roster_rows(
        start_date=start, end_date=end, window_days=90, emp_batch=50
    )
    new_df = pd.DataFrame(new_rows).drop_duplicates()
    print(f"✅ New roster rows: {len(new_df)}")

    existing = download_df(BLOB_ROSTER)
    if existing is not None and not existing.empty:
        combined = pd.concat([existing, new_df], ignore_index=True).drop_duplicates()
        print(f"✅ Combined: {len(combined)} rows (was {len(existing)})")
    else:
        combined = new_df

    upload_df(combined, BLOB_ROSTER)
    state["roster_last_end"] = end.isoformat()
    save_state(state)
    return combined


# ================================================================
# PHASE 2: TIMESHEET  (incremental, modified_since advances to today after each full fetch)
#
# Case A — cursor present (previous run interrupted):
#   Continue from the same modified_since; advance to today once complete.
#
# Case B — cursor is null (normal new run):
#   Use modified_since from state (advanced from last run).
#   Only fetches records changed since that date — typically a few pages.
#
# In both cases, modified_since is advanced to today (Perth midnight) after a full fetch.
# The next run will only pull changes after that point.
# ================================================================
def phase_timesheet(state: Dict[str, Any]) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("PHASE 2: Fetching Timesheets from OPMS")
    print("=" * 60)

    rtc            = _load_rtc()
    modified_since = state.get("timesheet_modified_since", INITIAL_MODIFIED_SINCE)

    # Build temporary checkpoint file
    checkpoint = {}
    if state.get("timesheet_cursor"):
        checkpoint = {
            "next_cursor":    state["timesheet_cursor"],
            "fetched_count":  state.get("timesheet_fetched_count", 0),
            "modified_since": modified_since,
        }
        print(f"🔁 Case A: resuming from cursor, already fetched ≈ {checkpoint['fetched_count']}")
    else:
        print(f"🆕 Case B: fresh incremental run")

    print(f"   modified_since = {modified_since}")

    tmp_dir = Path(os.environ.get("TEMP", os.environ.get("TMP", os.path.dirname(__file__))))
    tmp_cp  = tmp_dir / "opms_timesheets_checkpoint.json"
    tmp_cp.write_text(json.dumps(checkpoint), encoding="utf-8")

    all_ts = rtc.fetch_all_timesheets(
        modified_since=modified_since,
        page_size=25,
        checkpoint_file=tmp_cp,
        use_checkpoint=True
    )

    # Read checkpoint after fetch completes
    final_cp = {}
    if tmp_cp.exists():
        try:
            final_cp = json.loads(tmp_cp.read_text(encoding="utf-8"))
        except Exception:
            pass

    # Advance modified_since to today once fetch is complete (next_cursor is null)
    if not final_cp.get("next_cursor"):
        today_iso = perth_today_iso()
        state["timesheet_cursor"]         = None
        state["timesheet_fetched_count"]  = 0
        state["timesheet_modified_since"] = today_iso        # ← advance
        print(f"✅ Fetch complete, modified_since advanced to {today_iso}")
        print(f"   Next run will only fetch changes after {today_iso}")
    else:
        # Interrupted — save cursor and keep modified_since unchanged until fetch completes
        state["timesheet_cursor"]         = final_cp["next_cursor"]
        state["timesheet_fetched_count"]  = final_cp.get("fetched_count", 0)
        state["timesheet_modified_since"] = modified_since   # ← unchanged, advance after resume
        print(f"⏸  Interrupted — cursor saved, will resume next run")

    save_state(state)

    # Flatten (filter from initial date to avoid losing history)
    ts_df = rtc.flatten_to_long(all_ts, start_date=INITIAL_START_DATE.isoformat())
    print(f"✅ Timesheet rows this run: {len(ts_df)}")

    # Merge into existing timesheet (new values overwrite old, deduplicated by key)
    existing_ts = download_df(BLOB_TIMESHEET)
    if existing_ts is not None and not existing_ts.empty and not ts_df.empty:
        combined_ts = pd.concat([existing_ts, ts_df], ignore_index=True)
        combined_ts = combined_ts.groupby(
            ["date", "employee_id", "employee_name"], as_index=False
        )["hours"].sum()
        print(f"✅ Combined timesheet: {len(combined_ts)} rows (was {len(existing_ts)})")
    elif not ts_df.empty:
        combined_ts = ts_df
    else:
        print("ℹ️  No new timesheet data this run")
        combined_ts = existing_ts if existing_ts is not None else pd.DataFrame()

    if not combined_ts.empty:
        upload_df(combined_ts, BLOB_TIMESHEET)

    # Return only new rows for phase_merge incremental processing
    return ts_df


# ================================================================
# PHASE 3: MERGE  (incremental, only processes new keys from this run)
# ================================================================
def phase_merge(
    state: Dict[str, Any],
    rs_df: Optional[pd.DataFrame] = None,
    ts_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("PHASE 3: Incremental Merge")
    print("=" * 60)

    rtc   = _load_rtc()
    rs_df = rs_df if rs_df is not None else download_df(BLOB_ROSTER)

    # When running merge standalone, read full timesheet from blob
    if ts_df is None:
        ts_df = download_df(BLOB_TIMESHEET)

    if rs_df is None or rs_df.empty:
        raise RuntimeError("❌ Roster blob missing — run Phase 1 first")
    if ts_df is None or ts_df.empty:
        print("ℹ️  No new timesheet data, skipping merge")
        df = download_df(BLOB_MERGED)
        return df if df is not None else pd.DataFrame()

    # Identify (EmployeeID, Date) keys added or updated this run
    ts_norm = ts_df.rename(columns={"date": "Date", "employee_id": "EmployeeID"}).copy()
    ts_norm["Date"] = pd.to_datetime(ts_norm["Date"])
    new_keys = ts_norm[["EmployeeID", "Date"]].drop_duplicates()
    print(f"  New/updated keys: {len(new_keys)}")

    # Drop those keys from existing merged, preserving existing client/rate columns
    existing = download_df(BLOB_MERGED)
    if existing is not None and not existing.empty:
        existing["Date"] = pd.to_datetime(existing["Date"])
        exist_idx = pd.MultiIndex.from_frame(existing[["EmployeeID", "Date"]])
        new_idx   = pd.MultiIndex.from_frame(new_keys)
        keep      = existing[~exist_idx.isin(new_idx)]
        print(f"  Kept from existing: {len(keep)} rows")
    else:
        keep = pd.DataFrame()
        print("  No existing merged data — running full merge")

    delta = rtc.merge_timesheet_roster(ts_df, rs_df)
    print(f"  Delta: {len(delta)} rows")

    final = pd.concat([keep, delta], ignore_index=True) if not keep.empty else delta
    final.sort_values(["EmployeeID", "Date"], inplace=True, ignore_index=True)
    print(f"✅ Final merged: {len(final)} rows")

    upload_df(final, BLOB_MERGED)
    save_state(state)
    return final


# ================================================================
# PHASE 4: RESOLVE CLIENT
# ================================================================
def phase_resolve(state: Dict[str, Any]) -> None:
    print("\n" + "=" * 60)
    print("PHASE 4: Resolving resourceRequestClient")
    print("=" * 60)
    mod = _load_mod("Sharepoint_contracts", "Sharepoint_contracts.py")
    mod.main()
    save_state(state)


# ================================================================
# PHASE 5: RATE LOOKUP
# ================================================================
def phase_rates(state: Dict[str, Any]) -> None:
    print("\n" + "=" * 60)
    print("PHASE 5: Looking up Unit Rates")
    print("=" * 60)
    mod = _load_mod("Sharepoint_Rates", "Sharepoint_Rates.py")
    mod.main()
    save_state(state)


# ================================================================
# UPLOAD PROJECT MAP
# ================================================================
def upload_project_map(local_csv_path: str) -> None:
    print("\n📁 Uploading Project_Client_Map.csv...")
    upload_csv_file(local_csv_path, BLOB_PROJECT_MAP)


# ================================================================
# ENTRY POINT
# ================================================================
def main():
    parser = argparse.ArgumentParser(description="Timesheet Pipeline")
    parser.add_argument(
        "--phase",
        choices=["roster", "timesheet", "merge", "resolve", "rates", "all"],
        default="all"
    )
    parser.add_argument("--upload-map", metavar="CSV_PATH")
    args = parser.parse_args()

    if args.upload_map:
        upload_project_map(args.upload_map)
        return

    state      = load_state()
    start_time = datetime.now(tz=PERTH_TZ)
    print(f"\n🚀 Pipeline started at {perth_now_str()}")

    rs_df = ts_df = None

    try:
        if args.phase in ("all", "roster"):
            rs_df = phase_roster(state)

        if args.phase in ("all", "timesheet"):
            ts_df = phase_timesheet(state)

        if args.phase in ("all", "merge"):
            phase_merge(state, rs_df, ts_df)

        if args.phase in ("all", "resolve"):
            phase_resolve(state)

        if args.phase in ("all", "rates"):
            phase_rates(state)

    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        save_state(state)
        raise

    elapsed = datetime.now(tz=PERTH_TZ) - start_time
    print(f"\n🎉 Pipeline complete! Total time: {elapsed}")
    print(f"📦 blob://{AZURE_CONTAINER}/{BLOB_MERGED}")
    print(f"💾 blob://{AZURE_CONTAINER}/{BLOB_STATE}")


if __name__ == "__main__":
    main()
