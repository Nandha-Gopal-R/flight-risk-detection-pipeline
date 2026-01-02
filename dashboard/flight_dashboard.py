#!/usr/bin/env python3
"""
Stream-only Flight Risk dashboard (Streamlit)

- Reads parquet files from STREAM_PATH (pyarrow)
- Expected stream parquet columns (as produced by your scoring stream):
    'icao24' (flight id), 'latitude', 'longitude', 'evt_ts' (epoch seconds), 'predicted_risk'
- Date filters: Today / Yesterday / Custom range (IST = Asia/Kolkata)
- Map shows top-N risky flights (decimated for clarity)
- CSV export for flight list & selected flight track
"""

import os
from typing import List, Optional, Tuple, Dict
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta, time

import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import streamlit as st
import folium
from streamlit.components.v1 import html

# ---------------- CONFIG ----------------
# project-aware stream path (env override allowed)
PROJECT_ROOT = os.getenv(
    "PROJECT_ROOT",
    os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
)
DATA_DIR = os.getenv("DATA_PATH", os.path.join(PROJECT_ROOT, "data"))
STREAM_PATH = os.path.join(DATA_DIR, "gold", "risk_scores", "stream")

# actual column names present in your parquet files
COL_FLIGHT = "icao24"
COL_LAT = "latitude"
COL_LON = "longitude"
COL_TS = "evt_ts"             # epoch seconds (INT)
COL_RISK = "predicted_risk"

# canonical names used inside the app
CANON_FLIGHT = "flight_id"
CANON_LAT = "latitude"
CANON_LON = "longitude"
CANON_TS = "timestamp"
CANON_RISK = "predicted_risk"

IST = ZoneInfo("Asia/Kolkata")

# Map/plotting limits to reduce clutter
DEFAULT_TOP_N = 15
MAX_POINTS_PER_FLIGHT = 200  # sample/decimate if more points
MAP_ZOOM_START = 5
MAP_TILE = "cartodbpositron"

# ----------------- HELPERS -----------------
def find_parquet_files(path: str) -> List[str]:
    if not os.path.isdir(path):
        return []
    files = []
    for root, _, fs in os.walk(path):
        for f in fs:
            if f.endswith(".parquet"):
                files.append(os.path.join(root, f))
    return sorted(files)


def read_schema_names(path: str) -> List[str]:
    pf = pq.ParquetFile(path)
    return pf.schema.names


def safe_read_parquet_file(path: str, required: List[str]) -> Optional[pd.DataFrame]:
    """
    Try to read parquet file with pyarrow -> pandas. Return DataFrame if it contains required columns,
    else return None.
    """
    try:
        schema = read_schema_names(path)
    except Exception as e:
        # schema read failed (corrupt/unreadable) -> skip
        return None

    missing = [c for c in required if c not in schema]
    if missing:
        # file doesn't have the columns we need
        return None

    try:
        df = pd.read_parquet(path, engine="pyarrow", columns=required)
        return df
    except Exception:
        return None


@st.cache_data(ttl=60)
def load_stream_parquets(stream_dir: str) -> Tuple[pd.DataFrame, int]:
    """
    Load stream parquet files (only files containing required columns).
    Returns (df_all, skipped_count). Cached for 60 seconds.
    """
    files = find_parquet_files(stream_dir)
    if not files:
        return pd.DataFrame(), 0

    required = [COL_FLIGHT, COL_LAT, COL_LON, COL_TS, COL_RISK]
    dfs = []
    skipped = 0
    for p in files:
        df = safe_read_parquet_file(p, required)
        if df is None:
            skipped += 1
            continue
        dfs.append(df)
    if not dfs:
        return pd.DataFrame(), skipped
    df_all = pd.concat(dfs, ignore_index=True, sort=False)
    return df_all, skipped


def try_parse_timestamp_series(s: pd.Series) -> pd.Series:
    """
    Robust timestamp parsing for your stream:
    - evt_ts is epoch SECONDS (confirmed)
    - Parse with unit='s' and utc=True, then convert to IST
    """
    # if values are numeric (ints), parse as epoch seconds UTC
    if pd.api.types.is_numeric_dtype(s):
        ts = pd.to_datetime(s.astype("int64"), unit="s", errors="coerce", utc=True)
        ts = ts.dt.tz_convert(IST)
        return ts
    # fallback: try generic parsing, assume UTC if tz-naive
    series = pd.to_datetime(s, errors="coerce", utc=False)
    try:
        tz = series.dt.tz
    except Exception:
        tz = None
    if tz is None:
        # interpret naive as UTC then convert
        series = series.dt.tz_localize("UTC", ambiguous="NaT", nonexistent="NaT").dt.tz_convert(IST)
    else:
        series = series.dt.tz_convert(IST)
    return series


def normalize_risk(series: pd.Series) -> pd.Series:
    sn = pd.to_numeric(series, errors="coerce").fillna(0.0)
    try:
        q99 = float(np.nanpercentile(sn, 99)) if len(sn) else 0.0
    except Exception:
        q99 = sn.max() if len(sn) else 0.0
    if q99 > 1.0:
        sn = (sn / 100.0).clip(0.0, 1.0)
    return sn.clip(0.0, 1.0)


def decimate_coords(coords: List[Tuple[float, float]], max_points: int) -> List[Tuple[float, float]]:
    n = len(coords)
    if n <= max_points:
        return coords
    # uniform sampling including first and last points
    idx = np.linspace(0, n - 1, max_points, dtype=int)
    return [coords[i] for i in idx]


def risk_color(r: float) -> str:
    r = float(np.clip(r, 0.0, 1.0))
    red = int(255 * r)
    green = int(255 * (1 - r))
    return f"#{red:02x}{green:02x}00"


def get_date_range_for_mode(mode: str, custom_range: Optional[Tuple[datetime, datetime]] = None) -> Tuple[pd.Timestamp, pd.Timestamp]:
    """
    Returns (start_ts, end_ts) in IST timezone.
    mode in {"today", "yesterday", "custom"}
    For custom_range pass (date_from, date_to) as date objects or datetimes (interpreted in IST).
    Start = 00:00:00.000 on start_date, end = 23:59:59.999 on end_date in IST
    """
    now_ist = pd.Timestamp.now(tz=IST)
    if mode == "today":
        start = now_ist.normalize()
        end = (start + pd.Timedelta(days=1)) - pd.Timedelta(milliseconds=1)
    elif mode == "yesterday":
        start = (now_ist.normalize() - pd.Timedelta(days=1))
        end = (start + pd.Timedelta(days=1)) - pd.Timedelta(milliseconds=1)
    elif mode == "custom" and custom_range:
        start_date, end_date = custom_range
        start = pd.Timestamp(datetime.combine(start_date, time.min), tz=IST)
        end = pd.Timestamp(datetime.combine(end_date, time.max), tz=IST)
    else:
        start = now_ist.normalize()
        end = (start + pd.Timedelta(days=1)) - pd.Timedelta(milliseconds=1)
    return start, end


def filter_by_date(df: pd.DataFrame, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> pd.DataFrame:
    # expects df['timestamp'] to be tz-aware in IST
    mask = (df["timestamp"] >= start_ts) & (df["timestamp"] <= end_ts)
    return df.loc[mask].copy()


def summary_metrics(df: pd.DataFrame) -> Dict[str, object]:
    total_flights = int(df[CANON_FLIGHT].nunique())
    total_points = int(len(df))
    avg_risk = float(df[CANON_RISK].mean()) if len(df) else 0.0
    max_risk = float(df[CANON_RISK].max()) if len(df) else 0.0
    return {
        "total_flights": total_flights,
        "total_points": total_points,
        "avg_risk": avg_risk,
        "max_risk": max_risk,
    }


# ---------------- STREAMLIT UI ----------------
st.set_page_config(page_title="✈️ Flight Risk — Stream", layout="wide")
st.title("✈️ Flight Risk Dashboard — Stream")

st.markdown(f"**Stream path:** `{STREAM_PATH}`")
now_local = pd.Timestamp.now(tz=IST)
st.caption(f"Now (IST): {now_local.strftime('%Y-%m-%d %H:%M:%S %Z')}")

# --- Sidebar: date filters & map controls ---
with st.sidebar:
    st.header("Filters & options")

    date_mode = st.radio("Date filter", options=["Today", "Yesterday", "Custom range"], index=0)

    custom_from = None
    custom_to = None
    if date_mode == "Custom range":
        dr = st.date_input("Choose date range", value=(now_local.date(), now_local.date()))
        # date_input may return a single date or tuple
        if isinstance(dr, tuple) and len(dr) == 2:
            custom_from, custom_to = dr[0], dr[1]
        else:
            custom_from, custom_to = dr, dr

    st.markdown("---")
    st.subheader("Map options")
    top_n = st.number_input("Show top N flights by avg risk", min_value=1, max_value=200, value=DEFAULT_TOP_N)
    max_points = st.number_input("Max points per flight (decimate)", min_value=10, max_value=2000, value=MAX_POINTS_PER_FLIGHT)
    st.markdown("---")
    st.markdown("Actions")
    load_btn = st.button("Load / Refresh data")

# Show warnings if stream path missing
if not os.path.exists(STREAM_PATH):
    st.error(f"Stream directory not found: {STREAM_PATH}")
    st.stop()

# Load parquet data (cached)
if "df_stream" not in st.session_state or load_btn:
    with st.spinner("Loading stream parquet files..."):
        df_raw, skipped = load_stream_parquets(STREAM_PATH)
    st.session_state.df_stream = df_raw
    st.session_state.parquet_skipped = skipped

df_stream = st.session_state.get("df_stream", pd.DataFrame())
skipped = st.session_state.get("parquet_skipped", 0)

if df_stream.empty:
    st.warning(f"No stream rows loaded from {STREAM_PATH}. Parquet files skipped: {skipped}")
    st.stop()

# Ensure required columns exist in read result
required_cols = {COL_FLIGHT, COL_LAT, COL_LON, COL_TS, COL_RISK}
missing_req = required_cols - set(df_stream.columns.tolist())
if missing_req:
    st.error(f"Required columns missing from stream data: {missing_req}")
    st.stop()

# Normalize column names to canonical ones used in the app
df_stream = df_stream.rename(columns={
    COL_FLIGHT: CANON_FLIGHT,
    COL_LAT: CANON_LAT,
    COL_LON: CANON_LON,
    COL_TS: "evt_ts_raw",   # keep raw name so we can parse into canonical 'timestamp'
    COL_RISK: CANON_RISK
})

# parse timestamps and normalize risk
with st.spinner("Parsing timestamps and normalizing risk..."):
    df_stream["timestamp"] = try_parse_timestamp_series(df_stream["evt_ts_raw"])
    before = len(df_stream)
    df_stream[CANON_LAT] = pd.to_numeric(df_stream[CANON_LAT], errors="coerce")
    df_stream[CANON_LON] = pd.to_numeric(df_stream[CANON_LON], errors="coerce")
    df_stream[CANON_RISK] = normalize_risk(df_stream[CANON_RISK])
    df_stream = df_stream.dropna(subset=["timestamp", CANON_LAT, CANON_LON])
    dropped = before - len(df_stream)

# date filter window
mode_key = date_mode.lower()
custom_range = None
if date_mode == "Custom range" and custom_from and custom_to:
    custom_range = (custom_from, custom_to)

start_ts, end_ts = get_date_range_for_mode(mode_key, custom_range)

# apply filter by date
df_filt = filter_by_date(df_stream, start_ts, end_ts)

st.markdown(f"### Date range: {start_ts.strftime('%Y-%m-%d %H:%M:%S %Z')} → {end_ts.strftime('%Y-%m-%d %H:%M:%S %Z')}")
st.info(f"Loaded {len(df_stream):,} raw rows (dropped {dropped:,}). After date filter: {len(df_filt):,} rows. Skipped files: {skipped}")

if df_filt.empty:
    st.warning("No data for the selected date range. Try a larger range or check your stream files.")
    st.stop()

# Aggregate per-flight stats
flight_stats = (
    df_filt.groupby(CANON_FLIGHT, dropna=False)
    .agg(
        points=("timestamp", "count"),
        start_time=("timestamp", "min"),
        end_time=("timestamp", "max"),
        avg_risk=(CANON_RISK, "mean"),
        max_risk=(CANON_RISK, "max"),
    )
    .reset_index()
)

# choose top N flights by avg_risk then by points
flight_stats = flight_stats.sort_values(by=["avg_risk", "points"], ascending=[False, False])
top_flights = flight_stats.head(int(top_n))
top_ids = top_flights[CANON_FLIGHT].tolist()

# summary metrics
metrics = summary_metrics(df_filt)
col1, col2, col3, col4 = st.columns(4)
col1.metric("Flights", f"{metrics['total_flights']:,}")
col2.metric("Points", f"{metrics['total_points']:,}")
col3.metric("Avg risk", f"{metrics['avg_risk']:.3f}")
col4.metric("Max risk", f"{metrics['max_risk']:.3f}")

# Flight selection control in sidebar (compact)
with st.sidebar:
    st.markdown("---")
    st.subheader("Select flights to display")
    selected = st.multiselect("Select flights (empty = show top N)", options=flight_stats[CANON_FLIGHT].tolist(), default=top_ids)

if selected:
    display_ids = selected
else:
    display_ids = top_ids

# Build map centered on median of filtered data
center_lat = float(df_filt[CANON_LAT].median())
center_lon = float(df_filt[CANON_LON].median())

m = folium.Map(location=[center_lat, center_lon], zoom_start=MAP_ZOOM_START, tiles=MAP_TILE)

# draw only selected flights (decimated)
for fid in display_ids:
    sub = df_filt[df_filt[CANON_FLIGHT] == fid].sort_values("timestamp")
    if sub.empty:
        continue
    coords = list(zip(sub[CANON_LAT].tolist(), sub[CANON_LON].tolist()))
    if len(coords) < 2:
        continue
    coords_dec = decimate_coords(coords, int(max_points))
    avg_r = float(sub[CANON_RISK].mean())
    color = risk_color(avg_r)
    weight = 2 + 6 * avg_r
    popup_html = f"<b>Flight:</b> {fid}<br><b>Points:</b> {len(coords):,}<br><b>Avg risk:</b> {avg_r:.3f}"
    folium.PolyLine(coords_dec, color=color, weight=weight, opacity=0.9, popup=folium.Popup(popup_html, max_width=400)).add_to(m)
    # start/end markers (use original coord order)
    folium.CircleMarker(location=coords_dec[0], radius=3, color="green", fill=True).add_to(m)
    folium.CircleMarker(location=coords_dec[-1], radius=3, color="red", fill=True).add_to(m)

# legend (styled)
legend_html = """
 <div style="position: fixed;
             bottom: 50px; left: 50px; width: 240px; height: 110px;
             z-index:9999; font-size:14px; background: rgba(255,255,255,0.9);
             padding:8px; border-radius:6px;">
             <b>Risk legend</b><br>
             <i>green (low) → red (high)</i><br>
             <div style="display:flex; gap:6px; margin-top:6px;">
                <div style="background:#00ff00; width:40px; height:12px;"></div>
                <div style="background:#7fff00; width:40px; height:12px;"></div>
                <div style="background:#ffff00; width:40px; height:12px;"></div>
                <div style="background:#ff7f00; width:40px; height:12px;"></div>
                <div style="background:#ff0000; width:40px; height:12px;"></div>
             </div>
 </div>
 """
m.get_root().html.add_child(folium.Element(legend_html))

# Render map and flight table side-by-side
left_col, right_col = st.columns((2, 1))
with left_col:
    st.subheader("Map — Top flights")
    html(m._repr_html_(), height=720)

with right_col:
    st.subheader("Flight list (filtered)")
    display_table = flight_stats.copy()
    # format times for display (convert to IST strings)
    display_table["start_time"] = display_table["start_time"].dt.tz_convert(IST).dt.strftime("%Y-%m-%d %H:%M:%S")
    display_table["end_time"] = display_table["end_time"].dt.tz_convert(IST).dt.strftime("%Y-%m-%d %H:%M:%S")
    display_table = display_table.rename(columns={
        CANON_FLIGHT: "flight_id",
        "points": "points",
        "avg_risk": "avg_risk",
        "max_risk": "max_risk",
    }).reset_index(drop=True)

    st.dataframe(display_table, height=420)

    csv_bytes = display_table.to_csv(index=False).encode("utf-8")
    st.download_button("Download flights CSV", csv_bytes, file_name="flight_list.csv", mime="text/csv")

    st.markdown("---")
    st.subheader("Export a single flight track")
    flight_for_export = st.selectbox("Choose flight to export track", options=display_table["flight_id"].tolist())
    if flight_for_export:
        track_df = df_filt[df_filt[CANON_FLIGHT] == flight_for_export].sort_values("timestamp")
        # prepare export: convert timestamp to IST ISO
        track_df_export = track_df.copy()
        track_df_export["timestamp"] = track_df_export["timestamp"].dt.tz_convert(IST).dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        csv_track = track_df_export[[CANON_FLIGHT, CANON_LAT, CANON_LON, "timestamp", CANON_RISK]].to_csv(index=False).encode("utf-8")
        st.download_button(f"Download track CSV for {flight_for_export}", csv_track, file_name=f"track_{flight_for_export}.csv", mime="text/csv")

st.markdown("---")
st.caption("Stream-only dashboard. Date filtering uses IST (Asia/Kolkata). Parquet files missing required columns are skipped; check app warnings above.")
