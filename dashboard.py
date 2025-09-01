# frd_readiness_simple.py

import os, json
import pandas as pd
import streamlit as st
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv, find_dotenv

# ---------- Env (connection string + container only) ----------
load_dotenv(find_dotenv(), override=True)

CONTAINER   = os.getenv("BLOB_CONTAINER", "vectorlogs")
CONN_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

# ---------- Azure helpers ----------
@st.cache_resource(show_spinner=False)
def get_container_client():
    if not CONN_STRING:
        raise RuntimeError("Set AZURE_STORAGE_CONNECTION_STRING and BLOB_CONTAINER in your environment")
    return BlobServiceClient.from_connection_string(CONN_STRING).get_container_client(CONTAINER)

def list_blob_meta(prefix: str, max_blobs: int = 500):
    """Return [{name, last_modified}] newest first."""
    cc = get_container_client()
    rows = []
    for b in cc.list_blobs(name_starts_with=prefix):
        rows.append({"name": b.name, "last_modified": getattr(b, "last_modified", None)})
    rows.sort(key=lambda r: r["last_modified"] or pd.Timestamp.min, reverse=True)
    return rows[:max_blobs]

def read_blob_text(name: str) -> str:
    cc = get_container_client()
    return cc.download_blob(name).readall().decode("utf-8", errors="replace")

# ---------- Tiny parser (only your 6 fields) ----------
def parse_needed_fields(text: str) -> dict:
    """
    Extract only:
    Model, ServiceTag, TotalRAM, TPMError, DiskSize, InstallError
    from either JSON lines or logfmt lines.
    """
    out = {
        "Model": None,
        "ServiceTag": None,
        "TotalRAM": None,
        "TPMError": None,
        "DiskSize": None,
        "InstallError": None,
        "Success": True,  # flips to False if we see InstallSkipped/error
    }

    for line in text.splitlines():
        s = line.strip()
        if not s:
            continue

        # JSON line first
        if s.startswith("{") and s.endswith("}"):
            try:
                obj = json.loads(s)
                sysinfo = obj.get("sysinfo") or {}
                hw = sysinfo.get("Hardware") or {}
                mem = sysinfo.get("Memory") or {}

                if out["Model"] is None and isinstance(hw.get("Model"), str):
                    out["Model"] = hw["Model"]
                if out["ServiceTag"] is None:
                    stg = hw.get("ServiceTag") or obj.get("service_tag")
                    if isinstance(stg, str):
                        out["ServiceTag"] = stg
                if out["TotalRAM"] is None and mem.get("totalRAM") is not None:
                    out["TotalRAM"] = mem["totalRAM"]
                if out["DiskSize"] is None and "diskSize" in obj:
                    out["DiskSize"] = obj["diskSize"]
                if obj.get("msg") == "TPMChecked" and isinstance(obj.get("error"), str):
                    out["TPMError"] = obj["error"]
                if obj.get("msg") == "InstallSkipped" and isinstance(obj.get("error"), str):
                    out["InstallError"] = obj["error"]
                    out["Success"] = False
            except Exception:
                pass

        # logfmt tokens
        if out["Model"] is None and "sysinfo.Hardware.Model=" in s:
            out["Model"] = s.split("sysinfo.Hardware.Model=")[-1].split()[0].strip('"')
        if out["ServiceTag"] is None and "sysinfo.Hardware.ServiceTag=" in s:
            out["ServiceTag"] = s.split("sysinfo.Hardware.ServiceTag=")[-1].split()[0].strip('"')
        if out["TotalRAM"] is None and "sysinfo.Memory.totalRAM=" in s:
            out["TotalRAM"] = s.split("sysinfo.Memory.totalRAM=")[-1].split()[0].strip('"')
        if out["TPMError"] is None and "msg=TPMChecked" in s and "error=" in s:
            part = s.split('error="', 1)
            if len(part) > 1:
                out["TPMError"] = part[1].split('"', 1)[0]
        if out["DiskSize"] is None and "diskSize=" in s:
            out["DiskSize"] = s.split("diskSize=")[-1].split()[0].strip('"')
        if "msg=InstallSkipped" in s and "error=" in s:
            part = s.split('error="', 1)
            if len(part) > 1:
                out["InstallError"] = part[1].split('"', 1)[0]
                out["Success"] = False

        # early exit if we already have all 6 fields
        if all(k in out and out[k] is not None for k in ["Model","ServiceTag","TotalRAM","TPMError","DiskSize","InstallError"]):
            break

    return out

# ---------- UI ----------
st.set_page_config(page_title="FRD Readiness — Dry Run vs Install", layout="wide")
st.title("FRD Readiness — Dry Run vs Install")

def render_tab(prefix: str, title: str, max_blobs: int = 500):
    meta = list_blob_meta(prefix, max_blobs)
    if not meta:
        st.info(f"No blobs under `{prefix}`")
        return

    rows = []
    for m in meta:
        try:
            text = read_blob_text(m["name"])
            row = parse_needed_fields(text)
            # Date column from blob last_modified
            row["Date"] = pd.to_datetime(m["last_modified"]).strftime("%Y-%m-%d %H:%M:%S") if m["last_modified"] else None
            rows.append(row)
        except Exception as e:
            rows.append({
                "Model": None, "ServiceTag": None, "TotalRAM": None,
                "TPMError": f"READ ERROR: {e}", "DiskSize": None, "InstallError": None,
                "Success": False, "Date": None
            })

    df = pd.DataFrame(rows, columns=["Date","Model","ServiceTag","TotalRAM","TPMError","DiskSize","InstallError","Success"])

    # newest first
    if "Date" in df:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df.sort_values("Date", ascending=False, na_position="last")

    success_df = df[df["Success"]].drop(columns=["Success"])
    fail_df    = df[~df["Success"]].drop(columns=["Success"])

    st.subheader(f"{title} — Success")
    st.dataframe(success_df, use_container_width=True, height=300)

    st.subheader(f"{title} — Failures")
    st.dataframe(fail_df, use_container_width=True, height=300)

    st.caption(f"Scanned {len(meta)} blob(s) under `{prefix}`")

tab1, tab2 = st.tabs(["Dry Run", "Install"])
with tab1:
    render_tab("devices/dryrun/", "Dry Run")
with tab2:
    render_tab("devices/install/", "Install")
