# dashboard.py — Secrets-only, always auto-refresh, Dry Run / Install with Success & Fail tables (+ StoreName mapping)

import os
import json
import pandas as pd
import streamlit as st
from azure.storage.blob import BlobServiceClient
from streamlit_autorefresh import st_autorefresh

# ---------------- Secrets ----------------
def _get_secret(path: str, default=None):
    try:
        cur = st.secrets
        for p in path.split("."):
            cur = cur[p]
        return cur
    except Exception:
        return default

CONN_STRING = _get_secret("azure.connection_string")
CONTAINER   = _get_secret("azure.container")

def _mask(s: str | None, show=6):
    if not s:
        return "(missing)"
    return s[:show] + "…" if len(s) > show else s

# ---------------- CSV store map (ServiceTag -> StoreName) ----------------
CSV_CANDIDATES = [
    "StoreName vs ServiceTag.csv",
    "/mnt/data/StoreName vs ServiceTag.csv",
]

@st.cache_data(show_spinner=False)
def load_store_map():
    """
    Returns a dict {SERVICETAG -> StoreName}.
    Requires CSV with columns exactly: ServiceTag, StoreName.
    """
    path = None
    for cand in CSV_CANDIDATES:
        if os.path.exists(cand):
            path = cand
            break

    if not path:
        st.warning("Store map CSV not found (looked for: " + ", ".join([f"`{p}`" for p in CSV_CANDIDATES]) + ").")
        return {}

    try:
        df = pd.read_csv(path, usecols=["ServiceTag", "StoreName"])
    except Exception as e:
        st.warning(f"Failed to read store map CSV `{path}`: {e}")
        return {}

    # Normalize for matching
    df["ServiceTag"] = df["ServiceTag"].astype(str).str.strip().str.upper()
    df["StoreName"]  = df["StoreName"].astype(str).str.strip()

    mapping = dict(zip(df["ServiceTag"], df["StoreName"]))
    st.caption(f"📄 Loaded store map: {len(mapping)} entries from `{os.path.basename(path)}`")
    return mapping

STORE_MAP = load_store_map()

# ---------------- Azure helpers ----------------
@st.cache_resource(show_spinner=False)
def get_container_client():
    if not CONN_STRING or not CONTAINER:
        msg = (
            "Missing Streamlit secrets.\n\n"
            "Add `.streamlit/secrets.toml`:\n"
            "[azure]\n"
            "connection_string = \"BlobEndpoint=...;...;SharedAccessSignature=sv=...&sp=rl...\"\n"
            "container = \"vectorlogs\"\n\n"
            f"Detected -> connection_string: {_mask(CONN_STRING)}, container: {CONTAINER or '(missing)'}"
        )
        raise RuntimeError(msg)
    svc = BlobServiceClient.from_connection_string(CONN_STRING)
    return svc.get_container_client(CONTAINER)

def list_blob_meta(prefix: str, max_blobs: int = 500):
    """[{name, last_modified}] newest first; swallow errors to UI."""
    try:
        cc = get_container_client()
        rows = []
        for b in cc.list_blobs(name_starts_with=prefix):
            rows.append({"name": b.name, "last_modified": getattr(b, "last_modified", None)})
        rows.sort(
            key=lambda r: pd.to_datetime(r["last_modified"]) if r["last_modified"] else pd.Timestamp.min,
            reverse=True,
        )
        return rows[:max_blobs]
    except Exception as e:
        st.error(f"Listing blobs failed for `{prefix}`: {e}")
        return []

def read_blob_text(name: str) -> str:
    cc = get_container_client()
    return cc.download_blob(name).readall().decode("utf-8", errors="replace")

# ---------------- Minimal parser (6 fields) ----------------
def parse_needed_fields(text: str) -> dict:
    """
    Extract only: Model, ServiceTag, TotalRAM, TPMError, DiskSize, InstallError.
    Success=False if we see any ERROR-level line or an InstallSkipped with error.
    """
    out = {
        "Model": None,
        "ServiceTag": None,
        "TotalRAM": None,
        "TPMError": None,
        "DiskSize": None,
        "InstallError": None,
        "Success": True,
    }

    def _pick_error_from_logfmt(s: str) -> str | None:
        # Prefer error="..." if present
        if 'error="' in s:
            return s.split('error="', 1)[1].split('"', 1)[0]
        # Fallback: try msg=... token if present
        if "msg=" in s:
            part = s.split("msg=", 1)[1].split()[0]
            return part.strip()
        return None

    for line in text.splitlines():
        s = line.strip()
        if not s:
            continue

        # ---------- JSON line ----------
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
                    if isinstance(stg, str) and stg:
                        out["ServiceTag"] = stg

                if out["TotalRAM"] is None and mem.get("totalRAM") is not None:
                    out["TotalRAM"] = mem["totalRAM"]

                if out["DiskSize"] is None and "diskSize" in obj:
                    out["DiskSize"] = obj["diskSize"]

                # TPM error if present on TPMChecked
                if out["TPMError"] is None and obj.get("msg") == "TPMChecked" and isinstance(obj.get("error"), str):
                    out["TPMError"] = obj["error"]

                # InstallSkipped is a failure
                if obj.get("msg") == "InstallSkipped" and isinstance(obj.get("error"), str):
                    out["InstallError"] = obj["error"]
                    out["Success"] = False

                # Any JSON with level == ERROR is a failure
                lvl = str(obj.get("level") or "").upper()
                if lvl == "ERROR":
                    err = obj.get("error")
                    if isinstance(err, str) and err:
                        out["InstallError"] = err
                    else:
                        out["InstallError"] = obj.get("msg") or "ERROR"
                    out["Success"] = False
            except Exception:
                pass  # fall through to logfmt if not valid JSON

        # ---------- logfmt line ----------
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

        # InstallSkipped (logfmt)
        if "msg=InstallSkipped" in s and "error=" in s:
            err = s.split('error="', 1)[1].split('"', 1)[0]
            out["InstallError"] = err
            out["Success"] = False

        # Any ERROR-level line (logfmt)
        if "level=ERROR" in s:
            err = _pick_error_from_logfmt(s)
            if err:
                out["InstallError"] = err
            else:
                out["InstallError"] = "ERROR"
            out["Success"] = False

        # Early break if all fields collected and we already know failure status
        if all(out[k] is not None for k in ["Model","ServiceTag","TotalRAM","TPMError","DiskSize"]) and out["InstallError"] is not None:
            break

    return out

# ---------------- UI ----------------
st.set_page_config(page_title="FRD Readiness — Dry Run vs Install", layout="wide")

# Always auto-refresh every 5s
st_autorefresh(interval=5000, key="live_refresh")

st.title("FRD Readiness — Dry Run vs Install")
st.caption(f"Container: **{CONTAINER or '(missing)'}** • ConnStr(head): {_mask(CONN_STRING)} • Auto-refresh: 5s")

MAX_BLOBS = 500  # adjust if you like

def _normalize_st(series: pd.Series) -> pd.Series:
    """Upper + strip for ServiceTag to match CSV mapping."""
    return series.astype(str).str.strip().str.upper()

def _apply_store_map(df: pd.DataFrame) -> pd.DataFrame:
    """Add a 'StoreName' column from STORE_MAP using normalized ServiceTag."""
    if not isinstance(STORE_MAP, dict) or not STORE_MAP:
        df["StoreName"] = None
        return df
    norm = _normalize_st(df["ServiceTag"].fillna(""))
    mapped = norm.map(STORE_MAP)
    df["StoreName"] = mapped.where(pd.notna(mapped), None)
    return df

def render_tab(prefix: str, title: str, max_blobs: int = MAX_BLOBS):
    meta = list_blob_meta(prefix, max_blobs)
    if not meta:
        st.info(f"No blobs under `{prefix}`")
        return

    rows = []
    for m in meta:
        try:
            text = read_blob_text(m["name"])
            row = parse_needed_fields(text)
            row["Date"] = (
                pd.to_datetime(m["last_modified"]).strftime("%Y-%m-%d %H:%M:%S")
                if m["last_modified"] else None
            )
            rows.append(row)
        except Exception as e:
            rows.append({
                "Model": None, "ServiceTag": None, "TotalRAM": None,
                "TPMError": f"READ ERROR: {e}", "DiskSize": None, "InstallError": None,
                "Success": False, "Date": None
            })

    df = pd.DataFrame(
        rows,
        columns=["Date","Model","ServiceTag","TotalRAM","TPMError","DiskSize","InstallError","Success"]
    )

    if "Date" in df:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df.sort_values("Date", ascending=False, na_position="last")

    # Only keep the latest record per ServiceTag; split by latest state
    latest = df.sort_values("Date").drop_duplicates("ServiceTag", keep="last")

    # Attach StoreName from CSV mapping
    latest = _apply_store_map(latest)

    # Reorder columns for display
    display_cols = ["Date","StoreName","Model","ServiceTag","TotalRAM","TPMError","DiskSize","InstallError"]

    success_df = latest[latest["Success"]].drop(columns=["Success"]).reindex(columns=display_cols)
    fail_df    = latest[~latest["Success"]].drop(columns=["Success"]).reindex(columns=display_cols)

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
