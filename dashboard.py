# FRD ChromeOS — Readiness Dashboard (Streamlit secrets + fast refresh + ETag cache)

import os, json, traceback
from datetime import datetime, timezone
from typing import List, Optional, Tuple

import pandas as pd
import streamlit as st
from dateutil import parser as dtp
from azure.storage.blob import BlobServiceClient

# --------------------- Config via Streamlit Secrets ---------------------
def _get_secret(path: str, default=None):
    """
    Read nested keys from st.secrets like "azure.container".
    Fallback to environment variables using UPPER_SNAKE case.
    """
    try:
        # try secrets first
        parts = path.split(".")
        cur = st.secrets
        for p in parts:
            cur = cur[p]
        return cur
    except Exception:
        # env fallback: e.g. azure.container -> AZURE_CONTAINER, or known envs
        env_map = {
            "azure.container": "BLOB_CONTAINER",
            "azure.connection_string": "AZURE_STORAGE_CONNECTION_STRING",
            "azure.account_url": "BLOB_ACCOUNT_URL",
            "azure.sas_token": "BLOB_SAS_TOKEN",
            "app.max_blobs": "MAX_BLOBS",
            "app.max_events": "MAX_EVENTS",
            "app.file_patterns": "FILE_PATTERNS",
            "app.refresh_ms": "REFRESH_MS",
            "app.min_free_gb": "MIN_FREE_GB",
        }
        env_key = env_map.get(path, path.replace(".", "_").upper())
        return os.getenv(env_key, default)

CONTAINER     = _get_secret("azure.container", "vectorlogs")
CONN_STRING   = _get_secret("azure.connection_string", None)
ACCOUNT_URL   = _get_secret("azure.account_url", None)
SAS_TOKEN     = _get_secret("azure.sas_token", None)

DEFAULT_MAX_BLOBS  = int(_get_secret("app.max_blobs", 300))
DEFAULT_MAX_EVENTS = int(_get_secret("app.max_events", 100000))
FILE_PATTERNS      = _get_secret("app.file_patterns", ".log,.json,.ndjson")
REFRESH_MS         = int(_get_secret("app.refresh_ms", 3000))          # ms
MIN_FREE_GB        = float(_get_secret("app.min_free_gb", 16))

# --------------------- Azure helpers ---------------------
@st.cache_resource(show_spinner=False)
def get_container():
    if CONN_STRING:
        svc = BlobServiceClient.from_connection_string(CONN_STRING)
    elif ACCOUNT_URL and SAS_TOKEN:
        svc = BlobServiceClient(account_url=ACCOUNT_URL, credential=SAS_TOKEN)
    else:
        raise RuntimeError(
            "Configure Azure credentials via secrets:\n"
            "  [azure]\n  connection_string=...    # OR account_url + sas_token"
        )
    return svc.get_container_client(CONTAINER)

def list_blob_meta(prefix: str) -> List[Tuple[str, str, datetime, int]]:
    """[(name, etag, last_modified, size)] under prefix."""
    cc = get_container()
    out = []
    for b in cc.list_blobs(name_starts_with=prefix):
        out.append((b.name, getattr(b, "etag", None), getattr(b, "last_modified", None), getattr(b, "size", 0)))
    return out

def read_blob_text(name: str) -> str:
    cc = get_container()
    return cc.download_blob(name).readall().decode("utf-8", errors="replace")

# --------------------- JSON parsing ---------------------
def parse_json_text(text: str):
    """
    Return list[dict] from NDJSON / JSON array / single JSON object.
    Non-JSON lines are ignored silently.
    """
    lines = text.splitlines()
    if len(lines) > 1:
        out = []
        for ln in lines:
            s = ln.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
                if isinstance(obj, dict):
                    out.append(obj)
                elif isinstance(obj, list):
                    out.extend([x for x in obj if isinstance(x, dict)])
            except Exception:
                continue
        if out:
            return out
    try:
        obj = json.loads(text)
        if isinstance(obj, list):
            return [x for x in obj if isinstance(x, dict)]
        if isinstance(obj, dict):
            return [obj]
    except Exception:
        pass
    return []

def safe_dt(x):
    if x is None:
        return None
    if isinstance(x, (int, float)):
        try:
            return datetime.fromtimestamp(float(x), tz=timezone.utc)
        except Exception:
            return None
    if isinstance(x, str):
        try:
            return dtp.parse(x)
        except Exception:
            return None
    return None

# --------------------- Readiness extraction ---------------------
def extract_readiness_rows(events, blob_name: str) -> pd.DataFrame:
    rows = []
    for e in events:
        sysinfo = e.get("sysinfo") or {}
        hw = sysinfo.get("Hardware") or {}
        osinfo = sysinfo.get("OS") or {}
        disks = sysinfo.get("Disks") or []
        disk0 = disks[0] if disks else {}

        ts = safe_dt(e.get("time") or e.get("@timestamp") or e.get("timestamp"))
        level = e.get("level") or e.get("Level") or e.get("severity")
        msg = e.get("msg") or e.get("message") or e.get("Message")
        tag = e.get("service_tag") or hw.get("ServiceTag") or e.get("ServiceTag") or e.get("serviceTag")

        uefi = hw.get("isUsingUEFI")
        sboot = hw.get("safebootEnabled")
        tpm = hw.get("tpmSpecVersion")
        bit_enabled = disk0.get("BitLockerEnabled")
        bit_encrypted = disk0.get("BitLockerEncrypted")

        free = disk0.get("FreeSpace")
        total = disk0.get("TotalSize")

        free_gb, free_pct = None, None
        try:
            free_gb = round(int(free) / (1024**3), 1)
        except Exception:
            pass
        try:
            f = int(free); t = int(total) if total is not None else None
            free_pct = round((f / t) * 100, 1) if t and t > 0 else None
        except Exception:
            pass

        os_name = osinfo.get("FriendlyName"); os_ver = osinfo.get("Version")
        model = hw.get("Model"); family = hw.get("SystemFamily")

        active_ip = None
        for n in sysinfo.get("Networks") or []:
            ips = n.get("IPAddresses")
            if isinstance(ips, list) and ips:
                active_ip = ips[0]; break

        lbu = safe_dt(osinfo.get("LastBootUpTime"))
        boot_age_days = round((datetime.now(tz=lbu.tzinfo or timezone.utc) - lbu).total_seconds() / 86400, 1) if lbu else None

        tpm_ok = isinstance(tpm, str) and "2.0" in tpm
        bit_ok = (bit_enabled is False) and (bit_encrypted is False)
        free_ok = (free_gb is not None) and (free_gb >= MIN_FREE_GB)
        uefi_ok = (str(uefi).lower() in ("true","1","yes","on"))
        sboot_ok = (str(sboot).lower() in ("true","1","yes","on"))

        ready = all([uefi_ok, sboot_ok, tpm_ok, bit_ok, free_ok])

        rows.append({
            "_blob": blob_name,
            "time": ts,
            "level": level,
            "msg": msg,
            "service_tag": tag,

            "UEFI": uefi_ok if uefi is not None else None,
            "SecureBoot": sboot_ok if sboot is not None else None,
            "TPM2": tpm_ok if tpm is not None else None,
            "BitLockerOff": bit_ok if (bit_enabled is not None or bit_encrypted is not None) else None,
            "FreeGB": free_gb,
            "FreePct": free_pct,

            "OS": f"{os_name or ''} {os_ver or ''}".strip(),
            "Model": model,
            "Family": family,
            "ActiveIP": active_ip,
            "LastBoot": lbu,
            "BootAgeDays": boot_age_days,

            "Ready": ready
        })
    return pd.DataFrame(rows)

# --------------------- ETag cache (per-session) ---------------------
def get_cache():
    if "blob_cache" not in st.session_state:
        # blob_cache: { blob_name: { "etag": str, "rows": pd.DataFrame } }
        st.session_state.blob_cache = {}
    return st.session_state.blob_cache

def clear_cache():
    st.session_state.pop("blob_cache", None)
    st.cache_data.clear()

# --------------------- Load board (fast) ---------------------
@st.cache_data(ttl=1, show_spinner=False)
def _list_meta_cached(prefix: str):
    return list_blob_meta(prefix)

def load_board(prefix: str, service_tag_filter: Optional[str],
               patterns: str, max_blobs: int, max_events: int):
    """
    List + read with ETag cache. Only downloads blobs whose ETag changed.
    """
    cache = get_cache()
    path = prefix if not service_tag_filter else f"{prefix.rstrip('/')}/{service_tag_filter.strip('/')}/"

    meta = _list_meta_cached(path)  # [(name, etag, last_modified, size)]
    exts = [p.strip().lower() for p in patterns.split(",") if p.strip()]
    if exts:
        meta = [m for m in meta if any(m[0].lower().endswith(e) for e in exts)]

    # newest by name (yours start with date)
    meta.sort(key=lambda x: x[0])
    if max_blobs and len(meta) > max_blobs:
        meta = meta[-max_blobs:]

    scanned, frames, total = [], [], 0
    for name, etag, _, _ in meta:
        try:
            cached = cache.get(name)
            if cached and cached.get("etag") == etag:
                df_rows = cached["rows"]
            else:
                txt = read_blob_text(name)
                events = parse_json_text(txt)
                if not events:
                    continue
                df_rows = extract_readiness_rows(events, name)
                cache[name] = {"etag": etag, "rows": df_rows}

            if not df_rows.empty:
                frames.append(df_rows)
                scanned.append(name)
                total += len(df_rows)
                if total >= max_events:
                    break
        except Exception:
            continue

    if not frames:
        return pd.DataFrame(), scanned

    df = pd.concat(frames, ignore_index=True)
    df = df.sort_values("time", ascending=False, na_position="last")
    return df, scanned

# --------------------- UI helpers ---------------------
def badge(ok): return "✅ Ready" if bool(ok) else "❌ Not Ready"

def list_issues(row):
    miss = []
    if "UEFI" in row and row["UEFI"] is False: miss.append("UEFI")
    if "SecureBoot" in row and row["SecureBoot"] is False: miss.append("SecureBoot")
    if "TPM2" in row and row["TPM2"] is False: miss.append("TPM")
    if "BitLockerOff" in row and row["BitLockerOff"] is False: miss.append("BitLocker")
    try:
        if pd.notna(row.get("FreeGB")) and float(row["FreeGB"]) < MIN_FREE_GB:
            miss.append(f"Free<{int(MIN_FREE_GB)}GB")
    except Exception:
        pass
    return ", ".join(miss)

# --------------------- UI ---------------------
st.set_page_config(page_title="FRD Readiness Dashboard", layout="wide")
st.title("FRD ChromeOS — Readiness Dashboard (Live)")

with st.sidebar:
    st.markdown("### Source")
    st.write(f"Container: **{CONTAINER}**")
    tag_filter = st.text_input("Service Tag filter (optional)", placeholder="e.g. HD1701460800149").strip() or None

    st.markdown("### Limits")
    max_blobs = st.number_input("Max blobs to read (per tab)", 10, 10000, DEFAULT_MAX_BLOBS, 10)
    max_events = st.number_input("Max events to parse (total)", 1000, 1000000, DEFAULT_MAX_EVENTS, 1000)
    patterns = st.text_input("File extensions to include", value=FILE_PATTERNS)

    colb1, colb2 = st.columns(2)
    with colb1:
        if st.button("Force refresh (clear cache)"):
            clear_cache()
            st.experimental_rerun()
    with colb2:
        st.caption(f"Auto-refresh: every {REFRESH_MS/1000:.1f}s")
        if hasattr(st, "autorefresh"):
            st.autorefresh(interval=REFRESH_MS, key="auto")

def render_board(title: str, prefix: str):
    st.subheader(title)
    try:
        df, scanned = load_board(prefix, tag_filter, patterns, max_blobs, max_events)
    except Exception as e:
        st.error(f"Load failed: {e}")
        st.code(traceback.format_exc())
        return

    st.caption(
        f"Scanned **{len(scanned)}** file(s) under "
        f"`{prefix}{tag_filter or ''}` matching **{patterns}** • "
        f"Last update: {datetime.now().strftime('%H:%M:%S')}"
    )

    if df.empty:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Devices", 0); c2.metric("Ready", 0); c3.metric("Not Ready", 0); c4.metric("Errors", 0)
        with st.expander("Files scanned"):
            st.write(scanned if scanned else "(none)")
        st.info("No readiness data parsed yet.")
        return

    view = df.copy()
    view["Status"] = view["Ready"].map(badge)
    view["Issues"] = view.apply(list_issues, axis=1)

    # KPIs
    devices = view["service_tag"].nunique(dropna=True)
    ready = int(view["Ready"].sum())
    not_ready = int((~view["Ready"]).sum())
    errors = int((view["level"].astype(str).str.upper() == "ERROR").sum()) if "level" in view else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Devices", devices)
    c2.metric("Ready", ready)
    c3.metric("Not Ready", not_ready)
    c4.metric("Errors", errors)

    # Filters
    colf1, colf2, colf3 = st.columns(3)
    with colf1:
        fail_only = st.checkbox("Only show devices with issues", value=False, key=f"fail_only_{title}")
    with colf2:
        model_filter = st.text_input("Model contains", key=f"model_{title}")
    with colf3:
        os_filter = st.text_input("OS contains", key=f"os_{title}")

    if fail_only:
        view = view[view["Ready"] == False]
    if model_filter:
        view = view[view["Model"].astype(str).str.contains(model_filter, case=False, na=False)]
    if os_filter:
        view = view[view["OS"].astype(str).str.contains(os_filter, case=False, na=False)]

    # Sort: not ready first, more issues first, newest first
    view["issue_count"] = view["Issues"].fillna("").str.count(",").add(view["Issues"].ne("").astype(int))
    sort_by = [c for c in ["Ready", "issue_count", "time"] if c in view.columns]
    asc = [True, False, False][:len(sort_by)]
    if sort_by:
        view = view.sort_values(by=sort_by, ascending=asc)

    # Pretty formatting
    if "time" in view:
        view["time"] = pd.to_datetime(view["time"]).dt.tz_localize(None).dt.strftime("%Y-%m-%d %H:%M:%S")
    for col in ("FreeGB", "FreePct"):
        if col in view:
            view[col] = pd.to_numeric(view[col], errors="coerce").round(1)

    # Compact + details tables
    compact_cols = [c for c in [
        "time", "service_tag", "Status", "Issues",
        "FreeGB", "FreePct", "OS", "Model", "ActiveIP"
    ] if c in view.columns]
    detail_cols = [c for c in [
        "UEFI", "SecureBoot", "TPM2", "BitLockerOff", "BootAgeDays", "LastBoot",
        "level", "msg", "_blob"
    ] if c in view.columns]

    st.markdown("#### Devices & Readiness (clean view)")
    st.dataframe(view[compact_cols], use_container_width=True, height=420)

    with st.expander("Details (per-check booleans, messages, file)"):
        st.dataframe(view[compact_cols + detail_cols], use_container_width=True, height=420)

    st.download_button(
        "⬇️ Download clean view (CSV)",
        data=view[compact_cols + detail_cols].to_csv(index=False).encode("utf-8"),
        file_name=f"{title.lower().replace(' ','_')}_readiness_clean.csv",
        mime="text/csv"
    )

# --------------------- Tabs ---------------------
tab1, tab2 = st.tabs(["Dry Run", "Install"])
with tab1:
    render_board("Dry Run",  "devices/dryrun/")
with tab2:
    render_board("Install",  "devices/install/")
