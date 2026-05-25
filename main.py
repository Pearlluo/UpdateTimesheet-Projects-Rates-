"""
main.py
=======
主程序 — 按顺序串联所有模块，全部数据存到 Azure Blob。

状态管理:
  所有断点统一存在 blob: config/pipeline_state.json
  每次启动读档，跑完写档。

pipeline_state.json 结构:
  {
    "roster_last_end":           "2026-05-22",
    "timesheet_cursor":          null,
    "timesheet_fetched_count":   0,
    "timesheet_modified_since":  "2026-05-22T00:00:00Z",
    "updated_at":                "2026-05-22 14:29:31 AWST"
  }

增量策略:
  Phase 1: 读 roster_last_end，从它+1天拉到今天，拉完更新
  Phase 2: cursor 有值则续拉；cursor 为 null 则用 modified_since 拉增量
           拉完后 modified_since 推进到今天 Perth 0 点，下次只拉增量
  Phase 3: 只重算本次新增的 (EmployeeID, Date) key
  Phase 4/5: 子模块内部增量处理

用法:
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
# PERTH TIME  (AWST = UTC+8，无夏令时)
# ================================================================
PERTH_TZ = timezone(timedelta(hours=8))

def perth_today() -> date:
    return datetime.now(tz=PERTH_TZ).date()

def perth_now_str() -> str:
    return datetime.now(tz=PERTH_TZ).strftime("%Y-%m-%d %H:%M:%S AWST")

def perth_today_iso() -> str:
    """Perth 今天 0 点，作为 OPMS modified_since 参数"""
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

# 仅首次运行使用的初始默认值
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
# STATE — 读档 / 写档
# ================================================================
def load_state() -> Dict[str, Any]:
    data = download_bytes(BLOB_STATE)
    if data:
        try:
            state = json.loads(data.decode("utf-8"))
            print(f"📂 State loaded:")
            print(f"   roster_last_end          = {state.get('roster_last_end')}")
            print(f"   timesheet_modified_since = {state.get('timesheet_modified_since')}")
            print(f"   timesheet_cursor         = {'有（续拉）' if state.get('timesheet_cursor') else '无（新一轮）'}")
            print(f"   updated_at               = {state.get('updated_at')}")
            return state
        except Exception as e:
            print(f"⚠️  Failed to parse state JSON: {e}")

    print("🆕 No state file, initialising defaults")
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
    print(f"   timesheet_cursor         = {'有' if state.get('timesheet_cursor') else '无'}")


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
# PHASE 1: ROSTER  (增量，按日期)
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
# PHASE 2: TIMESHEET  (增量，modified_since 每次拉完推进到今天)
#
# 情况 A — cursor 有值（上次中断未拉完）:
#   沿用同一个 modified_since 继续拉，拉完后推进到今天
#
# 情况 B — cursor 为 null（正常新一轮）:
#   用 state 里的 modified_since（上次推进过的日期）拉增量
#   只拉那天之后有变动的记录，通常很少几页
#
# 两种情况拉完后都把 modified_since 推进到今天 Perth 0 点
# 下次跑就只拉今天之后的变动
# ================================================================
def phase_timesheet(state: Dict[str, Any]) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("PHASE 2: Fetching Timesheets from OPMS")
    print("=" * 60)

    rtc            = _load_rtc()
    modified_since = state.get("timesheet_modified_since", INITIAL_MODIFIED_SINCE)

    # 构建临时 checkpoint 文件
    checkpoint = {}
    if state.get("timesheet_cursor"):
        checkpoint = {
            "next_cursor":    state["timesheet_cursor"],
            "fetched_count":  state.get("timesheet_fetched_count", 0),
            "modified_since": modified_since,
        }
        print(f"🔁 情况A: cursor 续拉，already ≈ {checkpoint['fetched_count']}")
    else:
        print(f"🆕 情况B: 新一轮增量")

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

    # 读拉完后的 checkpoint
    final_cp = {}
    if tmp_cp.exists():
        try:
            final_cp = json.loads(tmp_cp.read_text(encoding="utf-8"))
        except Exception:
            pass

    # ── 关键：拉完（next_cursor=null）就把 modified_since 推进到今天 ──
    if not final_cp.get("next_cursor"):
        today_iso = perth_today_iso()
        state["timesheet_cursor"]         = None
        state["timesheet_fetched_count"]  = 0
        state["timesheet_modified_since"] = today_iso        # ← 推进
        print(f"✅ 拉完，modified_since 推进到 {today_iso}")
        print(f"   下次只拉 {today_iso} 之后有变动的记录")
    else:
        # 中断了，保存 cursor 等下次续拉，modified_since 保持不变
        state["timesheet_cursor"]         = final_cp["next_cursor"]
        state["timesheet_fetched_count"]  = final_cp.get("fetched_count", 0)
        state["timesheet_modified_since"] = modified_since   # ← 不变，续完再推进
        print(f"⏸  中断，cursor 已保存，下次续拉")

    save_state(state)

    # flatten（用初始日期过滤，确保不丢历史）
    ts_df = rtc.flatten_to_long(all_ts, start_date=INITIAL_START_DATE.isoformat())
    print(f"✅ Timesheet rows this run: {len(ts_df)}")

    # 合并到已有 timesheet（新增覆盖旧值，按 key 去重取最新）
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

    # 返回本次新增部分供 phase_merge 增量处理
    return ts_df


# ================================================================
# PHASE 3: MERGE  (增量，只处理本次新增 key)
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

    # 单独跑 merge 时从 blob 读全量 timesheet
    if ts_df is None:
        ts_df = download_df(BLOB_TIMESHEET)

    if rs_df is None or rs_df.empty:
        raise RuntimeError("❌ Roster blob missing — run Phase 1 first")
    if ts_df is None or ts_df.empty:
        print("ℹ️  No new timesheet data, skipping merge")
        df = download_df(BLOB_MERGED)
        return df if df is not None else pd.DataFrame()

    # 本次新增涉及的 (EmployeeID, Date)
    ts_norm = ts_df.rename(columns={"date": "Date", "employee_id": "EmployeeID"}).copy()
    ts_norm["Date"] = pd.to_datetime(ts_norm["Date"])
    new_keys = ts_norm[["EmployeeID", "Date"]].drop_duplicates()
    print(f"  New/updated keys: {len(new_keys)}")

    # 从现有 merged 里剔除这些 key，保留已有的 client/rate 列
    existing = download_df(BLOB_MERGED)
    if existing is not None and not existing.empty:
        existing["Date"] = pd.to_datetime(existing["Date"])
        exist_idx = pd.MultiIndex.from_frame(existing[["EmployeeID", "Date"]])
        new_idx   = pd.MultiIndex.from_frame(new_keys)
        keep      = existing[~exist_idx.isin(new_idx)]
        print(f"  Kept from existing: {len(keep)} rows")
    else:
        keep = pd.DataFrame()
        print("  No existing merged — full merge")

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