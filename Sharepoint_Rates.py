"""
rate_lookup.py
==============
Step 1  : Remove rows where resourceRequestClient = "C0000-Marlu" or contains "Z OH"
Step 2  : WorkType → DS / NS
Step 3  : Match unit rates from JMS-Rates → write to UnitRate column (incremental: skip rows already rated)

Incremental strategy:
  - Rows that already have a UnitRate value are kept as-is
  - Only unrated rows are matched against SharePoint
  - SharePoint (PPL-Positions / JMS-Rates) is only called when there are pending rows
"""

import io
import os
import requests
import pandas as pd
from typing import Optional, List, Dict, Tuple
from dotenv import load_dotenv

from azure.storage.blob import BlobServiceClient

load_dotenv()

# ─── BLOB CONFIG ──────────────────────────────────────────────────────────────

AZURE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_CONTAINER         = os.getenv("AZURE_BLOB_CONTAINER", "timesheethour")
BLOB_MERGED             = "data/merged.parquet"


def _blob_client(blob_path: str):
    svc = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    return svc.get_blob_client(container=AZURE_CONTAINER, blob=blob_path)


def download_df_from_blob(blob_path: str) -> pd.DataFrame:
    data = _blob_client(blob_path).download_blob().readall()
    return pd.read_parquet(io.BytesIO(data), engine="pyarrow")


def upload_df_to_blob(df: pd.DataFrame, blob_path: str) -> None:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    _blob_client(blob_path).upload_blob(buf, overwrite=True)
    print(f"✅ Uploaded → blob://{AZURE_CONTAINER}/{blob_path}  ({len(df)} rows)")


# ─── SHAREPOINT CONFIG ────────────────────────────────────────────────────────

TENANT_ID     = os.getenv("SHAREPOINT_TENANT_ID")
CLIENT_ID     = os.getenv("SHAREPOINT_CLIENT_ID")
CLIENT_SECRET = os.getenv("SHAREPOINT_CLIENT_SECRET")
SP_HOST       = os.getenv("SHAREPOINT_HOST")
SITE_NAME     = os.getenv("SITE_NAME")
RATES_LIST    = os.getenv("LIST_NAME1", "JMS-Rates")

# ─── AUTH ────────────────────────────────────────────────────────────────────

def get_access_token() -> str:
    resp = requests.post(
        f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token",
        data={"client_id": CLIENT_ID, "client_secret": CLIENT_SECRET,
              "scope": "https://graph.microsoft.com/.default",
              "grant_type": "client_credentials"}
    )
    if not resp.ok:
        print("❌ TOKEN ERROR:", resp.text)
    resp.raise_for_status()
    return resp.json()["access_token"]

# ─── GRAPH HELPERS ───────────────────────────────────────────────────────────

def get_site_id(token: str) -> str:
    resp = requests.get(
        f"https://graph.microsoft.com/v1.0/sites/{SP_HOST}:/sites/{SITE_NAME}",
        headers={"Authorization": f"Bearer {token}"}
    )
    resp.raise_for_status()
    sid = resp.json()["id"]
    print(f"✅ Site ID: {sid}")
    return sid


def get_list_id(token: str, site_id: str, list_name: str) -> str:
    resp = requests.get(
        f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists",
        headers={"Authorization": f"Bearer {token}"}
    )
    resp.raise_for_status()
    target = list_name.strip().lower()
    for item in resp.json().get("value", []):
        dn = item.get("displayName", "").strip()
        nm = item.get("name", "").strip()
        if dn.lower() == target or nm.lower() == target:
            print(f"✅ Found list: {dn}")
            return item["id"]
    available = [i.get("displayName") for i in resp.json().get("value", [])]
    raise Exception(f"List '{list_name}' not found. Available: {available}")


def fetch_all_list_items(token: str, site_id: str, list_id: str) -> List[Dict]:
    headers = {"Authorization": f"Bearer {token}"}
    all_items = []
    url = (f"https://graph.microsoft.com/v1.0/sites/{site_id}"
           f"/lists/{list_id}/items"
           f"?$expand=fields($select=*)&$top=5000")
    while url:
        resp = requests.get(url, headers=headers)
        if not resp.ok:
            print("❌ FETCH ERROR:", resp.text)
        resp.raise_for_status()
        data = resp.json()
        all_items.extend(data.get("value", []))
        print(f"  Pulled {len(data.get('value', []))} rows (total {len(all_items)})")
        url = data.get("@odata.nextLink")
    return all_items

# ─── STEP 2: WORKTYPE → DS / NS ──────────────────────────────────────────────

def classify_shift(worktype: str) -> str:
    wt = str(worktype).strip().upper()
    if wt in {"FLY IN AM", "FLY OUT AM", "DRIVE IN AM", "DRIVE OUT AM"} \
            or wt.endswith(" AM"):
        return "NS"
    if wt == "NIGHT SHIFT":
        return "NS"
    return "DS"

# ─── STEP 3: BUILD RATES MAPS ────────────────────────────────────────────────

def build_rates_map(token: str, site_id: str) -> Tuple[Dict, Dict]:
    # PPL-Positions: item id → Title (uppercase to avoid case mismatch)
    print("\nFetching PPL-Positions...")
    pos_list_id = get_list_id(token, site_id, "PPL-Positions")
    pos_items   = fetch_all_list_items(token, site_id, pos_list_id)

    pos_id_to_title: Dict[str, str] = {}
    for item in pos_items:
        item_id = str(item.get("id", "")).strip()
        title   = str(item.get("fields", {}).get("Title", "")).strip().upper()
        if item_id and title:
            pos_id_to_title[item_id] = title
    print(f"  → {len(pos_id_to_title)} positions loaded")

    # JMS-Rates
    print(f"\nFetching '{RATES_LIST}'...")
    rates_list_id = get_list_id(token, site_id, RATES_LIST)
    rates_items   = fetch_all_list_items(token, site_id, rates_list_id)

    specific: Dict[Tuple, float] = {}
    default:  Dict[Tuple, float] = {}

    for item in rates_items:
        f           = item.get("fields", {})
        project_id  = str(f.get("ProjectID") or "").strip().upper()
        pos_lid     = str(f.get("PositionLookupId") or "").strip()
        pos_title   = pos_id_to_title.get(pos_lid, "")
        day_shift   = f.get("DayShift")
        night_shift = f.get("NightShift")

        if not pos_title:
            continue

        if project_id:
            if day_shift is not None:
                specific[(project_id, pos_title, "DS")] = day_shift
            if night_shift is not None:
                specific[(project_id, pos_title, "NS")] = night_shift
        else:
            if day_shift is not None:
                default[(pos_title, "DS")] = day_shift
            if night_shift is not None:
                default[(pos_title, "NS")] = night_shift

    print(f"  → {len(specific)} project-specific rates")
    print(f"  → {len(default)} default rates")
    return specific, default


def lookup_rate(project_client: str, position: str, shift: str,
                specific: Dict, default: Dict) -> Optional[float]:
    proj_id  = str(project_client).strip().upper().split("-")[0]
    pos_up   = str(position).strip().upper()
    shift_up = shift.upper()

    rate = specific.get((proj_id, pos_up, shift_up))
    if rate is not None:
        return rate
    return default.get((pos_up, shift_up))

# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    # ── Load merged.parquet ───────────────────────────────────────
    print("Loading merged.parquet from Blob...")
    df = download_df_from_blob(BLOB_MERGED)
    print(f"  Loaded {len(df)} rows")

    # ── Step 1: Remove C0000-Marlu and Z OH ──────────────────────
    before     = len(df)
    client_col = df["resourceRequestClient"].astype(str).str.strip()
    drop_mask  = (
        (client_col.str.upper() == "C0000-MARLU") |
        (client_col.str.upper().str.contains(r"\bZ\s*OH\b", regex=True, na=False))
    )
    df = df[~drop_mask].reset_index(drop=True)
    print(f"Step 1: Removed {before - len(df)} rows → {len(df)} remaining")

    # ── Step 2: WorkType → DS / NS (recalculate all rows to cover new entries) ──
    df["Shift"] = df["WorkType"].apply(classify_shift)
    print(f"Step 2: DS={(df['Shift']=='DS').sum()}, NS={(df['Shift']=='NS').sum()}")

    # ── Step 3: Incremental rate matching ────────────────────────
    # Ensure UnitRate column exists as object dtype to allow None and float together
    if "UnitRate" not in df.columns:
        df["UnitRate"] = pd.array([None] * len(df), dtype=object)  # ← fix: object dtype
    else:
        # Cast existing column to object so None can be assigned alongside floats
        df["UnitRate"] = df["UnitRate"].astype(object)             # ← fix: cast to object

    # Split: already rated vs pending
    rated_mask    = df["UnitRate"].notna()
    rated_count   = rated_mask.sum()
    pending_count = (~rated_mask).sum()
    print(f"Step 3: Already rated={rated_count}, Pending={pending_count}")

    if pending_count == 0:
        print("✅ All rows already rated, skipping SharePoint call")
        upload_df_to_blob(df, BLOB_MERGED)
        return

    # Only connect to SharePoint for pending rows
    print("\nConnecting to SharePoint...")
    token   = get_access_token()
    site_id = get_site_id(token)
    specific, default = build_rates_map(token, site_id)

    print("\nMatching unit rates for pending rows...")
    df.loc[~rated_mask, "UnitRate"] = df[~rated_mask].apply(
        lambda row: lookup_rate(
            row.get("resourceRequestClient", ""),
            row.get("RRA_Position", ""),
            row.get("Shift", "DS"),
            specific, default
        ), axis=1
    )

    matched   = df["UnitRate"].notna().sum()
    unmatched = df["UnitRate"].isna().sum()
    print(f"  Matched:   {matched} rows")
    print(f"  Unmatched: {unmatched} rows")

    if unmatched > 0:
        miss = (
            df[df["UnitRate"].isna()][["resourceRequestClient", "RRA_Position", "Shift"]]
            .drop_duplicates()
            .head(20)
        )
        print("\n  Unmatched combos (first 20):")
        print(miss.to_string(index=False))

    # ── Write back to Blob ────────────────────────────────────────
    print(f"\nWriting back to Blob...")
    upload_df_to_blob(df, BLOB_MERGED)
    print(f"\nDone ✅  UnitRate filled: {matched} / {len(df)} rows")


if __name__ == "__main__":
    main()
