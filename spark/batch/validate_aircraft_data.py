#!/usr/bin/env python3
"""
validate_aircraft_data.py

Spark batch job that validates cleaned aircraft parquet, writes metrics,
and saves quarantined bad rows for inspection.

Outputs:
  /home/nandha/bigdata/app/output/data_quality/metrics/parquet/   -> metrics parquet (append)
  /home/nandha/bigdata/app/output/data_quality/metrics/json/      -> latest metrics JSON (overwritten)
  /home/nandha/bigdata/app/output/data_quality/quarantine/<rule>/ -> quarantined rows (parquet)

Exit codes:
  0 -> SUCCESS (no issues)
  1 -> WARNING (issues found, data still usable)
  2 -> FAIL (critical: missing input or schema mismatch)
"""

import sys
import json
from datetime import datetime
from pathlib import Path
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, lit, current_timestamp
)
from pyspark.sql.types import StringType

# ---------------- CONFIG ----------------
PROJECT_ROOT = os.getenv(
    "PROJECT_ROOT",
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
)
DATA_DIR = os.getenv("DATA_PATH", os.path.join(PROJECT_ROOT, "data"))

INPUT_DIR = os.path.join(DATA_DIR, "silver", "aircraft_cleaned")
OUT_BASE = os.path.join(DATA_DIR, "validation")
METRICS_PARQUET = os.path.join(OUT_BASE, "metrics", "parquet")
METRICS_JSON = os.path.join(OUT_BASE, "metrics", "json", "last_metrics.json")
QUARANTINE_BASE = os.path.join(OUT_BASE, "quarantine")
# thresholds (tweak as needed)
MAX_NULL_RATIO = 0.05   # if more than 5% nulls -> warning
# ----------------------------------------

def build_spark():
    return SparkSession.builder \
        .appName("validate_aircraft_data") \
        .master("local[*]") \
        .getOrCreate()

def safe_write_parquet(df, path):
    # use append for metrics/quarantine to keep history if desired
    df.write.mode("append").option("compression", "snappy").parquet(path)

def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Basic checks for input dir
    inp = Path(INPUT_DIR)
    if not inp.exists() or not any(inp.iterdir()):
        print(f"❌ FAIL: Input path missing or empty: {INPUT_DIR}")
        spark.stop()
        sys.exit(2)

    print("✅ Reading cleaned parquet:", INPUT_DIR)
    try:
        df = spark.read.parquet(INPUT_DIR)
    except Exception as e:
        print("❌ FAIL: Error reading parquet:", e)
        spark.stop()
        sys.exit(2)

    total = df.count()
    print("✅ Total rows:", total)
    if total == 0:
        print("❌ FAIL: No rows to validate.")
        spark.stop()
        sys.exit(2)

    # Required columns
    required = {"icao24", "latitude", "longitude", "timestamp", "date"}
    missing = required - set(df.columns)
    if missing:
        print("❌ FAIL: Missing required columns:", missing)
        spark.stop()
        sys.exit(2)
    print("✅ Schema contains required columns")

    # Null checks (latitude, longitude, timestamp)
    nulls = df.filter(
        col("latitude").isNull() |
        col("longitude").isNull() |
        col("timestamp").isNull()
    )
    null_count = nulls.count()
    null_ratio = null_count / total
    print(f"✅ Null count: {null_count} (ratio {null_ratio:.4f})")

    # Range checks
    range_viol = df.filter(
        (col("latitude") < -90.0) | (col("latitude") > 90.0) |
        (col("longitude") < -180.0) | (col("longitude") > 180.0)
    )
    range_count = range_viol.count()
    print("✅ Range violations (lat/lon):", range_count)

    # Duplicate detection: same icao24 + timestamp
    dupe_groups = df.groupBy("icao24", "timestamp").count().filter(col("count") > 1)
    dupe_count_groups = dupe_groups.count()
    dupe_total_rows = dupe_groups.agg({"count": "sum"}).collect()[0][0] or 0
    print("✅ Duplicate groups:", dupe_count_groups, " (rows involved approx):", dupe_total_rows)

    # Simple business rules checks
    status = "SUCCESS"
    messages = []
    if null_ratio > MAX_NULL_RATIO:
        status = "WARNING"
        messages.append(f"High null ratio: {null_ratio:.3f}")
    if range_count > 0:
        status = "WARNING"
        messages.append(f"{range_count} rows with out-of-range lat/lon")
    if dupe_count_groups > 0:
        status = "WARNING"
        messages.append(f"{dupe_count_groups} duplicate groups")

    # Write quarantines only if issues found (keeps output small)
    ts = datetime.now().astimezone().strftime("%Y%m%dT%H%M%SZ")
    if null_count > 0:
        qpath = f"{QUARANTINE_BASE}/nulls/date={ts}"
        print("🗂 Writing null rows to quarantine:", qpath)
        safe_write_parquet(nulls.withColumn("quarantine_ts", lit(ts)), qpath)

    if range_count > 0:
        qpath = f"{QUARANTINE_BASE}/range_violations/date={ts}"
        print("🗂 Writing range-violations to quarantine:", qpath)
        safe_write_parquet(range_viol.withColumn("quarantine_ts", lit(ts)), qpath)

    if dupe_count_groups > 0:
        # capture offending rows (join original df with dupe_groups)
        dupe_details = df.join(dupe_groups.select("icao24", "timestamp"), on=["icao24", "timestamp"], how="inner")
        qpath = f"{QUARANTINE_BASE}/duplicates/date={ts}"
        print("🗂 Writing duplicate rows to quarantine:", qpath)
        safe_write_parquet(dupe_details.withColumn("quarantine_ts", lit(ts)), qpath)

    # Build metrics row
    metrics = [{
    "run_ts": datetime.now().astimezone().isoformat(),
    "total_rows": int(total),
    "null_count": int(null_count),
    "null_ratio": float(null_ratio),
    "range_count": int(range_count),
    "duplicate_groups": int(dupe_count_groups),
    "status": status,
    "messages": " | ".join(messages) if messages else ""
}]


    # Save metrics parquet (append) and JSON (overwrite latest)
    metrics_df = spark.createDataFrame(metrics)
    print("📝 Writing metrics parquet to:", METRICS_PARQUET)
    safe_write_parquet(metrics_df.withColumn("written_ts", current_timestamp()), METRICS_PARQUET)

    # Also write a small JSON file for easy human reading
    Path(METRICS_JSON).parent.mkdir(parents=True, exist_ok=True)
    with open(METRICS_JSON, "w", encoding="utf-8") as jf:
        json.dump(metrics[0], jf, indent=2)
    print("📝 Wrote human-readable metrics JSON to:", METRICS_JSON)

    print("✅ Validation complete. Status:", status)
    spark.stop()

    # Exit semantics for Airflow:
    # - SUCCESS / WARNING -> exit 0 (pipeline continues)
    # - FAIL              -> exit 1 (pipeline stops)
    if status == "FAIL":
        sys.exit(1)

    sys.exit(0)

if __name__ == "__main__":
    main()
