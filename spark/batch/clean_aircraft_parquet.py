#!/usr/bin/env python3
"""
clean_aircraft_parquet.py

Robust batch cleaner for mixed-parquet timestamp files.
Reads raw parquet at DEFAULT_INPUT_DIR, cleans and writes to DEFAULT_OUTPUT_DIR
(partitioned by date). Uses a safe file-by-file fallback when Spark's bulk read
fails due to mixed physical encodings (INT96 vs string).

Usage:
  ~/bigdata/spark/bin/spark-submit --master local[*] ~/bigdata/app/spark/clean_aircraft_parquet.py
"""
from pathlib import Path
import sys
import os
import traceback

from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import DoubleType
from functools import reduce

# Optional (fallback)
try:
    import pyarrow.parquet as pq
    import pandas as pd
    PYARROW_AVAILABLE = True
except Exception:
    PYARROW_AVAILABLE = False

# ---------- CONFIG ----------
PROJECT_ROOT = os.getenv(
    "PROJECT_ROOT",
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
)
DATA_DIR = os.getenv("DATA_PATH", os.path.join(PROJECT_ROOT, "data"))

DEFAULT_INPUT_DIR = os.path.join(DATA_DIR, "bronze", "aircraft_positions")
DEFAULT_OUTPUT_DIR = os.path.join(DATA_DIR, "silver", "aircraft_cleaned")
TMP_VIEW_NAME = "_tmp_loaded_parts"
# ----------------------------

def build_spark(app_name="clean_aircraft_parquet"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        # tolerate INT96 rebase issues if present
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.mergeSchema", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

def list_parquet_files(input_dir: Path):
    files = []
    if not input_dir.exists():
        return files
    for p in input_dir.rglob("*.parquet"):
        # skip hidden/temp files that start with '.' or '_'
        if p.name.startswith(".") or p.name.startswith("_"):
            continue
        files.append(p)
    files.sort()
    return files

def read_bulk_safe(spark, input_dir: str):
    """
    Try one-shot read (fast). Return DataFrame or raise.
    """
    # allow Parquet reader to try and reconcile when possible; this may still fail.
    # we already set int96Rebase configs in Spark session.
    return spark.read.option("mergeSchema", "true").parquet(input_dir)

def read_file_with_pyarrow(path: Path):
    """
    Use pyarrow to read a single parquet file into pandas then normalize timestamp column.
    Returns pandas.DataFrame or raises.
    """
    if not PYARROW_AVAILABLE:
        raise RuntimeError("pyarrow not available for fallback read.")
    table = pq.read_table(str(path))
    df = table.to_pandas()

    # Normalize timestamp column if present
    if "timestamp" in df.columns:
        # If INT types (likely epoch seconds)
        if pd.api.types.is_integer_dtype(df["timestamp"]):
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", errors="coerce")
        else:
            # try parsing strings or pyarrow timestamps
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    # Ensure date column exists or create from timestamp if possible
    if "date" not in df.columns:
        if "timestamp" in df.columns:
            df["date"] = df["timestamp"].dt.date
        else:
            # fallback to None
            df["date"] = pd.NaT

    return df

def pandas_to_spark(spark, pdf):
    """
    Convert pandas df to spark DataFrame with safe schema inference.
    """
    # convert date column to string if it's datetime.date; spark will coerce later
    if "date" in pdf.columns:
        pdf["date"] = pdf["date"].astype(str)
    return spark.createDataFrame(pdf)

def read_per_file_fallback(spark, input_dir: Path):
    """
    Read each parquet file individually; try spark.read first, fallback to pyarrow->pandas.
    Return one Spark DataFrame representing concatenation.
    """
    files = list_parquet_files(input_dir)
    if not files:
        raise RuntimeError(f"No parquet files found under {input_dir}")

    parts = []
    for f in files:
        try:
            # Try reading the single file with Spark (fast)
            df_part = spark.read.parquet(str(f))
            print(f"Loaded (spark) {f.name} rows={df_part.count()}")
        except Exception as e:
            print(f"Spark failed to read {f.name}: {e}")
            if not PYARROW_AVAILABLE:
                print("pyarrow not available — cannot fallback. Aborting.")
                raise
            try:
                pdf = read_file_with_pyarrow(f)
                # quick normalization of column names to match pipeline expectation
                # keep common set: icao24, lat/latitude, lon/longitude, flight, reg, timestamp, date
                # remap if needed: if file has 'lat' rename to 'latitude', similarly 'lon'->'longitude'
                if "lat" in pdf.columns and "latitude" not in pdf.columns:
                    pdf = pdf.rename(columns={"lat":"latitude"})
                if "lon" in pdf.columns and "longitude" not in pdf.columns:
                    pdf = pdf.rename(columns={"lon":"longitude"})
                # create missing columns with None
                for colname in ("icao24","latitude","longitude","flight","reg","timestamp","date"):
                    if colname not in pdf.columns:
                        pdf[colname] = None
                # convert pandas timestamp to proper timezone-naive datetime
                if "timestamp" in pdf.columns:
                    pdf["timestamp"] = pd.to_datetime(pdf["timestamp"], errors="coerce")
                df_part = pandas_to_spark(spark, pdf)
                print(f"Loaded (pyarrow->pandas->spark) {f.name} rows={df_part.count()}")
            except Exception as e2:
                print(f"FALLBACK READ FAILED for {f.name}: {e2}")
                traceback.print_exc()
                # skip this file rather than abort whole job
                continue

        parts.append(df_part)

    if not parts:
        raise RuntimeError("No parts were successfully read (all failed).")

    # union all parts by name, allowing missing columns to be added as nulls
    def union_all(dfa, dfb):
        return dfa.unionByName(dfb, allowMissingColumns=True)

    combined = reduce(union_all, parts)
    return combined

def normalize_and_clean(df):
    # Normalize icao24 fallback to flight/reg unknown, lowercase trimmed
    df2 = df.withColumn("icao24",
        F.lower(F.coalesce(F.trim(F.col("icao24")),
                           F.trim(F.col("flight")),
                           F.trim(F.col("reg")),
                           F.lit("unknown")))
    )

    # ensure numeric lat/lon
    df2 = df2.withColumn("latitude", F.col("latitude").cast(DoubleType())) \
             .withColumn("longitude", F.col("longitude").cast(DoubleType()))

    # drop bad coords
    df2 = df2.filter(
        (F.col("latitude").isNotNull()) & (F.col("longitude").isNotNull()) &
        (F.col("latitude") >= -90.0) & (F.col("latitude") <= 90.0) &
        (F.col("longitude") >= -180.0) & (F.col("longitude") <= 180.0)
    )

    # ensure timestamp is timestamp type: try cast if possible
    # sometimes timestamp is string; if it's numeric cast to seconds->timestamp
    df2 = df2.withColumn(
        "_ts_from_long",
        F.when(F.col("timestamp").cast("long").isNotNull(),
               F.from_unixtime(F.col("timestamp").cast("long")).cast("timestamp"))
         .otherwise(F.lit(None).cast("timestamp"))
    ).withColumn(
        "_ts_from_str",
        F.to_timestamp(F.col("timestamp"))
    ).withColumn(
        "timestamp",
        F.coalesce(F.col("_ts_from_long"), F.col("_ts_from_str"), F.current_timestamp())
    ).drop("_ts_from_long", "_ts_from_str")

    # derive date for partitioning
    df2 = df2.withColumn("date", F.to_date(F.col("timestamp")))

    # select final columns
    result = df2.select(
        F.col("icao24"),
        F.col("latitude"),
        F.col("longitude"),
        F.col("flight"),
        F.col("reg"),
        F.col("timestamp"),
        F.col("date")
    )

    # dedupe example: keep latest row per (icao24, timestamp) (if duplicates exist)
    w = Window.partitionBy("icao24", "timestamp").orderBy(F.col("timestamp").desc())
    final = result.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
    return final

def main():
    input_dir = Path(DEFAULT_INPUT_DIR)
    output_dir = Path(DEFAULT_OUTPUT_DIR)

    spark = build_spark()

    # Attempt fast bulk read first
    df = None
    try:
        print("Trying bulk read (fast path)...")
        df = read_bulk_safe(spark, str(input_dir))
        print("Bulk read successful.")
    except Exception as e:
        print("Bulk read failed. Falling back to file-by-file loader.")
        print("Error:", e)
        # fallback
        df = read_per_file_fallback(spark, input_dir)

    print("Raw count (sample):", df.count())
    df.printSchema()

    print("Cleaning / normalizing...")
    cleaned = normalize_and_clean(df)
    print("Clean count (after filters):", cleaned.count())

    # ensure output parent exists
    output_dir.mkdir(parents=True, exist_ok=True)

    # Write cleaned parquet (append safe; we use overwrite to start fresh)
    # remove temporary _temporary folder if exists (Spark can leave that behind)
    temp_dir = output_dir / "_temporary"
    if temp_dir.exists():
        try:
            import shutil
            shutil.rmtree(str(temp_dir))
        except Exception:
            pass

    print(f"Writing cleaned parquet to {output_dir} partitioned by date ...")
    cleaned.write.mode("append").option("compression", "snappy").partitionBy("date").parquet(str(output_dir))
    print("WRITE COMPLETE.")
    spark.stop()

if __name__ == "__main__":
    main()
