#!/usr/bin/env python3
"""
feature_engineer_ml.py

Batch ML feature engineering from SILVER parquet (per-observation features).
Produces parquet of per-interval ML features partitioned by flight_date.

Outputs columns:
  - icao24 (string)
  - flight_date (date)
  - delta_t (long)            # seconds between consecutive observations
  - movement_score (double)   # movement proxy (smoothed)
  - risk_label (int)          # 0/1 rule-based label
"""
import argparse
import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, lag, unix_timestamp, when, abs, to_date, avg, max, count, coalesce
)
from pyspark.sql.types import DoubleType, LongType

def spark_session():
    return (
        SparkSession.builder
        .appName("FlightRiskFeatureEngineering")
        .master("local[*]")
        .getOrCreate()
    )

def main(input_path, out_path):
    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"✅ Reading SILVER data from {input_path}")
    df = spark.read.option("mergeSchema", "true").parquet(input_path)

    # Ensure timestamp is timestamp type
    # If timestamp is epoch seconds (long), convert, otherwise to_timestamp will handle strings
    from pyspark.sql.functions import to_timestamp
    df = df.withColumn("timestamp", to_timestamp(col("timestamp")))

    # add flight_date for partitioning and to avoid mixing days
    df = df.withColumn("flight_date", to_date(col("timestamp")))

    # Window per aircraft ordered by time
    w = Window.partitionBy("icao24").orderBy(col("timestamp").asc())

    # previous lat/lon and timestamp
    df2 = df \
        .withColumn("prev_ts", lag(col("timestamp")).over(w)) \
        .withColumn("prev_lat", lag(col("latitude")).over(w)) \
        .withColumn("prev_lon", lag(col("longitude")).over(w))

    # delta_t in seconds (null for first observation)
    df3 = df2.withColumn("delta_t", (unix_timestamp(col("timestamp")) - unix_timestamp(col("prev_ts"))).cast("long"))

    # movement_score: a simple proxy (keep existing approach)
    df4 = df3.withColumn(
        "movement_score",
        (abs(col("latitude") - col("prev_lat")) + abs(col("longitude") - col("prev_lon"))).cast(DoubleType())
    )

    # risk label (rule-based)
    df5 = df4.withColumn(
        "risk_label",
        when((col("delta_t") > 600) | (col("movement_score") < 0.001), 1).otherwise(0)
    )

    # final features: keep only rows that have a valid delta_t (i.e., not first observation)
    features = df5.select(
        col("icao24"),
        col("flight_date"),
        col("delta_t").cast(LongType()).alias("delta_t"),
        col("movement_score").cast(DoubleType()).alias("movement_score"),
        col("risk_label").cast("int").alias("risk_label")
    ).where(col("delta_t").isNotNull())

    # write partitioned by flight_date for easy versioning / selection
    print("✅ Writing ML features to:", out_path)
    features.write.mode("overwrite").partitionBy("flight_date").parquet(out_path)

    print("✅ Feature engineering completed")
    print("✅ Rows:", features.count())
    spark.stop()

if __name__ == "__main__":
    PROJECT_ROOT = os.getenv(
        "PROJECT_ROOT",
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    )
    DATA_DIR = os.getenv("DATA_PATH", os.path.join(PROJECT_ROOT, "data"))

    parser = argparse.ArgumentParser(description="ML Feature Engineering (Batch)")
    parser.add_argument("--input", "-i",
                        default=os.path.join(DATA_DIR, "silver", "aircraft_cleaned"),
                        help="Path to silver (cleaned) parquet input")
    parser.add_argument("--out", "-o",
                        default=os.path.join(DATA_DIR, "ml", "training_data", "flight_features"),
                        help="Output features parquet path")
    args = parser.parse_args()

    main(args.input, args.out)
