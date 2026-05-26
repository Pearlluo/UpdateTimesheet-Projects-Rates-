"""
resolve_client.py
=================
Step 0  : Data cleanup — DISABLED, no rows removed
Step 1  : Map Project → resourceRequestClient via Project_Client_Map.csv
Step 2  : For rows still unmatched, query SharePoint via Graph API:

            Confirmed SP field structure:
              JMS-Jobs    : JobID="SH-25006", ProjectLookupId="6"
              JMS-Projects: id="6",           ATitle="C0002-Newmont"

            Chain:
              Excel[Project] prefix "SH-25006"
                → JMS-Jobs[JobID] match
                → JMS-Jobs[ProjectLookupId] = "6"
                  → JMS-Projects[id] = "6"
                    → JMS-Projects[ATitle] = "C0002-Newmont"  ← resourceRequestClient

Step 3  : Manual overrides
Step 4  : Write result back to Blob (merged.parquet)

Changes:
  - MAIN_XLSX / MAP_CSV local paths → Blob read/write
  - pd.read_excel / pd.ExcelWriter → download_df_from_blob / upload_df_to_blob
  - pd.read_csv(MAP_CSV) → download_csv_from_blob
  - All business logic, SharePoint calls, and step comments unchanged
  - Step 0 fully disabled — no rows are removed for any reason
"""

import io
import os
import re
import requests
import pandas as pd
from typing import Optional, List, Dict
from dotenv import load_dotenv

from azure.storage.blob import BlobServiceClient

load_dotenv()

# ─── BLOB CONFIG ──────────────────────────────────────────────────────────────

AZURE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_CONTAINER         = os.getenv("AZURE_BLOB_CONTAINER", "timesheethour")
BLOB_MERGED             = "data/merged.parquet"
BLOB_PROJECT_MAP        = "config/Project_Client_Map.csv"


def _blob_client(blob_path: str):
    svc = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    return svc.get_blob_client(container=AZURE_CONTAINER, blob=blob_path)


def download_df_from_blob(blob_path: str) -> pd.DataFrame:
    data = _blob_client(blob_path).download_blob().readall()
    return pd.read_parquet(io.BytesIO(data), engine="pyarrow")


def download_csv_from_blob(blob_path: str) -> pd.DataFrame:
    data = _blob_client(blob_path).download_blob().readall()
    return pd.read_csv(io.BytesIO(data))


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
JOBS_LIST     = os.getenv("GAP_LIST_NAME", "JMS-Jobs")
PROJ_LIST     = os.getenv("LIST_NAME",     "JMS-Projects")

# ─── MANUAL OVERRIDES ────────────────────────────────────────────────────────

MANUAL_OVERRIDES = {
    "SH-25031-Full Plant Shutdown": "C0049-Newmont Boddington CW125779",
}

# ─── PREFIX EXTRACTOR ────────────────────────────────────────────────────────

PREFIX_RE = re.compile(r"^([A-Z]{2,4}-\d{4,6})", re.IGNORECASE)

def extract_prefix(s: str) -> Optional[str]:
    """'SH-25006 - July FPS'  →  'SH-25006'"""
    m = PREFIX_RE.match(str(s).strip())
    return m.group(1).upper() if m else None

# ─── AUTH ────────────────────────────────────────────────────────────────────

def get_access_token() -> str:
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    payload = {
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope":         "https://graph.microsoft.com/.default",
        "grant_type":    "client_credentials",
    }
    response = requests.post(url, data=payload)
    if not response.ok:
        print("❌ TOKEN ERROR")
        print(response.text)
    response.raise_for_status()
    return response.json()["access_token"]

# ─── GRAPH HELPERS ───────────────────────────────────────────────────────────

def get_site_id(token: str) -> str:
    url = f"https://graph.microsoft.com/v1.0/sites/{SP_HOST}:/sites/{SITE_NAME}"
    response = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    if not response.ok:
        print("❌ SITE ERROR:", response.text)
    response.raise_for_status()
    site_id = response.json()["id"]
    print(f"✅ Site ID: {site_id}")
    return site_id


def get_list_id(token: str, site_id: str, list_name: str) -> str:
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists"
    response = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    if not response.ok:
        print("❌ LIST FETCH ERROR:", response.text)
    response.raise_for_status()

    target = list_name.strip().lower()
    for item in response.json().get("value", []):
        display_name  = item.get("displayName", "").strip()
        internal_name = item.get("name", "").strip()
        if display_name.lower() == target or internal_name.lower() == target:
            print(f"✅ Found list: {display_name}")
            return item["id"]

    available = [i.get("displayName") for i in response.json().get("value", [])]
    raise Exception(f"List '{list_name}' not found. Available: {available}")


def fetch_all_list_items(token: str, site_id: str, list_id: str) -> List[Dict]:
    headers = {"Authorization": f"Bearer {token}"}
    all_items = []
    url = (
        f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
        f"?$expand=fields&$top=5000"
    )
    while url:
        response = requests.get(url, headers=headers)
        if not response.ok:
            print("❌ FETCH ITEMS ERROR:", response.text)
        response.raise_for_status()
        data  = response.json()
        batch = data.get("value", [])
        all_items.extend(batch)
        print(f"  Pulled {len(batch)} rows (total {len(all_items)})")
        url = data.get("@odata.nextLink")
    return all_items

# ─── BUILD LOOKUP MAPS ────────────────────────────────────────────────────────

def build_sp_maps(token: str, site_id: str) -> Dict[str, str]:
    """
    Returns job_map: { "SH-25006": "C0002-Newmont", ... }

    Step 1: JMS-Projects → { item_id: ATitle }
            e.g. { "6": "C0002-Newmont", "64": "C0060-Hancock Shuts" }

    Step 2: JMS-Jobs → prefix → ProjectLookupId → ATitle
            e.g. "SH-25006" → "6" → "C0002-Newmont"
    """
    # Step 1: JMS-Projects → id_to_atitle
    print(f"\nFetching '{PROJ_LIST}'...")
    proj_list_id = get_list_id(token, site_id, PROJ_LIST)
    proj_items   = fetch_all_list_items(token, site_id, proj_list_id)

    id_to_atitle: Dict[str, str] = {}
    for item in proj_items:
        item_id = str(item.get("id") or "").strip()
        fields  = item.get("fields", {})
        atitle  = str(fields.get("ATitle") or "").strip()
        if item_id and atitle:
            id_to_atitle[item_id] = atitle

    print(f"  → {len(id_to_atitle)} projects loaded")
    print(f"  Sample: { dict(list(id_to_atitle.items())[:5]) }")

    # Step 2: JMS-Jobs → job prefix → ATitle
    print(f"\nFetching '{JOBS_LIST}'...")
    jobs_list_id = get_list_id(token, site_id, JOBS_LIST)
    jobs_items   = fetch_all_list_items(token, site_id, jobs_list_id)

    job_map: Dict[str, str] = {}
    for item in jobs_items:
        fields    = item.get("fields", {})
        job_id    = str(fields.get("JobID") or "").strip()
        lookup_id = str(fields.get("ProjectLookupId") or "").strip()

        prefix = extract_prefix(job_id)
        atitle = id_to_atitle.get(lookup_id, "")

        if prefix and atitle:
            job_map.setdefault(prefix, atitle)

    print(f"  → {len(job_map)} job prefixes mapped")
    print(f"  Sample: { dict(list(job_map.items())[:5]) }")

    return job_map

# ─── RESOLVE SINGLE PROJECT ───────────────────────────────────────────────────

def sp_lookup(excel_project: str, job_map: Dict[str, str]) -> Optional[str]:
    """
    "SH-25006 - JULY FPS" → prefix "SH-25006" → "C0002-Newmont"
    """
    prefix = extract_prefix(excel_project)
    if not prefix:
        return None
    return job_map.get(prefix) or None

# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    # ── Load merged parquet from Blob ─────────────────────────────────────────
    print("Loading merged.parquet from Blob...")
    df = download_df_from_blob(BLOB_MERGED)
    print(f"  Loaded {len(df)} rows")

    # ── Step 0: DISABLED — no rows removed ───────────────────────────────────
    print(f"Step 0: Skipped — all {len(df)} rows retained")

    # ── Load CSV map from Blob ────────────────────────────────────────────────
    print("\nLoading CSV map from Blob...")
    mp = download_csv_from_blob(BLOB_PROJECT_MAP)

    df["Project"] = df["Project"].astype(str).str.strip()
    mp["resourceRequestProject"] = mp["resourceRequestProject"].astype(str).str.strip()
    mp["resourceRequestClient"]  = mp["resourceRequestClient"].astype(str).str.strip()

    proj_to_client = (
        mp.dropna(subset=["resourceRequestProject", "resourceRequestClient"])
          .drop_duplicates(subset=["resourceRequestProject"])
          .set_index("resourceRequestProject")["resourceRequestClient"]
          .to_dict()
    )

    # ── Step 1: CSV map ───────────────────────────────────────────────────────
    df["resourceRequestClient"] = df["Project"].map(proj_to_client).fillna("")
    csv_matched = (df["resourceRequestClient"] != "").sum()
    print(f"Step 1: CSV map matched {csv_matched} / {len(df)} rows")

    # ── Step 2: SharePoint fallback ───────────────────────────────────────────
    unmatched_mask     = df["resourceRequestClient"] == ""
    unmatched_projects = df.loc[unmatched_mask, "Project"].unique()
    print(f"Step 2: {unmatched_mask.sum()} unmatched rows "
          f"({len(unmatched_projects)} unique projects) → querying SharePoint...")

    if unmatched_mask.any():
        token   = get_access_token()
        site_id = get_site_id(token)
        job_map = build_sp_maps(token, site_id)

        sp_cache: Dict[str, str] = {}
        for proj in unmatched_projects:
            result         = sp_lookup(proj, job_map)
            sp_cache[proj] = result or ""
            status = f"✓ {result}" if result else "✗ not found"
            print(f"  {str(proj):<55} → {status}")

        df.loc[unmatched_mask, "resourceRequestClient"] = (
            df.loc[unmatched_mask, "Project"].map(sp_cache).fillna("")
        )
        sp_matched = sum(1 for v in sp_cache.values() if v)
        print(f"       SharePoint matched: {sp_matched} / {len(unmatched_projects)} projects")

    # ── Step 3: Manual overrides ──────────────────────────────────────────────
    for proj_key, client_val in MANUAL_OVERRIDES.items():
        mask = df["Project"] == proj_key
        if mask.any():
            df.loc[mask, "resourceRequestClient"] = client_val
            print(f"Step 3: Override {proj_key!r} → {client_val!r}")

    # ── Step 4: Write back to Blob ────────────────────────────────────────────
    print(f"\nStep 4: Writing back to Blob...")
    upload_df_to_blob(df, BLOB_MERGED)

    total_filled = (df["resourceRequestClient"] != "").sum()
    print(f"\nDone ✅  resourceRequestClient filled: {total_filled} / {len(df)} rows")


if __name__ == "__main__":
    main()
