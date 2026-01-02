#!/usr/bin/env python3
"""
spark_sql_analytics.py

Batch Spark SQL analytics for the flight dataset (cleaned parquet).

Writes results to --output/<query_name> as parquet and csv.

Usage:
  ~/bigdata/spark/bin/spark-submit --master local[*] \
    ~/bigdata/app/spark/spark_sql_analytics.py \
    --input /home/nandha/bigdata/app/output/aircraft_cleaned \
    --output /home/nandha/bigdata/app/output/aircraft_aggregated_sql
"""

import argparse
import os
from pathlib import Path

from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F

def build_spark(app_name="spark_sql_analytics"):
    return SparkSession.builder.appName(app_name).master("local[*]").getOrCreate()

def write_result(df, outdir: Path, name: str, partition_cols=None):
    target = outdir / name
    csv_target = outdir / f"{name}_csv"
    # create parent
    target.parent.mkdir(parents=True, exist_ok=True)
    # write parquet (overwrite so re-running replaces)
    if partition_cols:
        df.write.mode("overwrite").option("compression", "snappy").partitionBy(*partition_cols).parquet(str(target))
    else:
        df.write.mode("overwrite").option("compression", "snappy").parquet(str(target))
    # also write CSV for easy preview
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(csv_target))
    print(f"  -> wrote parquet: {target}")
    print(f"  -> wrote csv: {csv_target}")

def main(input_dir: str, output_dir: str):
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    in_path = Path(input_dir)
    out_path = Path(output_dir)

    if not in_path.exists():
        raise SystemExit(f"Input path not found: {in_path}")

    print("✅ Reading cleaned parquet:", in_path)
    df = spark.read.option("mergeSchema", "true").parquet(str(in_path))
    print("  Columns:", df.columns)
    print("  Count (sample read):", df.count())

    # register SQL view
    df.createOrReplaceTempView("aircraft_cleaned")

    # ========== Query 1: Flights per day ==========
    q1 = """
    SELECT date, COUNT(*) AS total_flights
    FROM aircraft_cleaned
    GROUP BY date
    ORDER BY date
    """
    df_q1 = spark.sql(q1)
    print("✅ Query flights_per_day: rows =", df_q1.count())
    write_result(df_q1, out_path, "flights_per_day")

    # ========== Query 2: Unique aircraft per day ==========
    q2 = """
    SELECT date, COUNT(DISTINCT icao24) AS unique_aircraft
    FROM aircraft_cleaned
    GROUP BY date
    ORDER BY date
    """
    df_q2 = spark.sql(q2)
    print("✅ Query unique_aircraft_per_day: rows =", df_q2.count())
    write_result(df_q2, out_path, "unique_aircraft_per_day")

    # ========== Query 3: Flights per hour ==========
    # use hour(timestamp)
    df_q3 = spark.sql("""
    SELECT date, hour(timestamp) AS hour, COUNT(*) AS total_flights
    FROM aircraft_cleaned
    GROUP BY date, hour
    ORDER BY date, hour
    """)
    print("✅ Query flights_per_hour: rows =", df_q3.count())
    write_result(df_q3, out_path, "flights_per_hour")

    # ========== Query 4: Top aircraft by sightings ==========
    df_q4 = spark.sql("""
    SELECT icao24, COUNT(*) AS sightings
    FROM aircraft_cleaned
    GROUP BY icao24
    ORDER BY sightings DESC
    LIMIT 100
    """)
    print("✅ Query top_aircraft_by_sightings: rows =", df_q4.count())
    write_result(df_q4, out_path, "top_aircraft_by_sightings")

    # ========== Query 5: Busiest grid cells (lat/lon buckets) ==========
    # Create lat/lon buckets by rounding to 1 decimal (about ~11km). Adjust as needed.
    df_bucket = df.withColumn("lat_bucket", (F.floor(F.col("latitude") * 10) / 10).cast("double")) \
                  .withColumn("lon_bucket", (F.floor(F.col("longitude") * 10) / 10).cast("double"))
    df_q5 = df_bucket.groupBy("lat_bucket", "lon_bucket") \
                     .agg(F.count("*").alias("records")) \
                     .orderBy(F.desc("records"))
    print("✅ Query busiest_grid: rows =", df_q5.count())
    write_result(df_q5, out_path, "busiest_grid")

    # ========== Query 6: Estimated speed per flight (approx) ==========
    # compute distance between consecutive points per aircraft, compute speed (knots), then avg speed per aircraft.
    # Haversine implemented with Spark SQL functions (no UDF) for portability.
    # window to get previous point
    w = Window.partitionBy("icao24").orderBy("timestamp")

    df_speed = df.select(
        "icao24", "latitude", "longitude", "timestamp"
    ).withColumn("lat_prev", F.lag("latitude").over(w)) \
     .withColumn("lon_prev", F.lag("longitude").over(w)) \
     .withColumn("ts_prev", F.lag("timestamp").over(w)) \
     .withColumn("ts_seconds", F.col("timestamp").cast("long")) \
     .withColumn("ts_prev_seconds", F.col("ts_prev").cast("long"))

    # compute haversine using SQL functions
    # convert degrees to radians inside formula
    R = 6371.0  # km
    to_rad = lambda c: (F.radians(F.col(c)))

    df_speed2 = df_speed.withColumn("phi1", F.radians(F.col("lat_prev"))) \
        .withColumn("phi2", F.radians(F.col("latitude"))) \
        .withColumn("dphi", F.radians(F.col("latitude") - F.col("lat_prev"))) \
        .withColumn("dlambda", F.radians(F.col("longitude") - F.col("lon_prev")))

    df_speed3 = df_speed2.withColumn("a",
        F.sin(F.col("dphi")/2) * F.sin(F.col("dphi")/2) +
        F.cos(F.col("phi1")) * F.cos(F.col("phi2")) *
        F.sin(F.col("dlambda")/2) * F.sin(F.col("dlambda")/2)
    ).withColumn("c", 2 * F.asin(F.sqrt(F.col("a")))) \
     .withColumn("dist_km", F.col("c") * F.lit(R))

    # time delta hours
    df_speed4 = df_speed3.withColumn("dt_hours",
        (F.col("ts_seconds") - F.col("ts_prev_seconds")) / F.lit(3600.0)
    )

    # nautical miles = dist_km / 1.852 ; speed (knots) = nm / dt_hours
    df_speed5 = df_speed4.withColumn("speed_kts",
        F.when((F.col("dt_hours") > 0) & (F.col("dist_km").isNotNull()),
               (F.col("dist_km") / F.lit(1.852)) / F.col("dt_hours"))
        .otherwise(F.lit(None))
    )

    # keep only rows where we could compute speed
    df_speed_valid = df_speed5.filter(F.col("speed_kts").isNotNull())

    # average speed per aircraft (also count of intervals)
    df_q6 = df_speed_valid.groupBy("icao24") \
        .agg(
            F.count("*").alias("intervals"),
            F.avg("speed_kts").alias("avg_speed_kts"),
            F.expr("percentile_approx(speed_kts, 0.5)").alias("median_speed_kts")
        ).orderBy(F.desc("avg_speed_kts"))

    print("✅ Query estimated_speed_per_flight (rows) =", df_q6.count())
    write_result(df_q6, out_path, "estimated_speed_per_flight")

    print("All queries finished. Outputs in:", out_path)
    spark.stop()

if __name__ == "__main__":
    PROJECT_ROOT = os.getenv(
        "PROJECT_ROOT",
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    )
    DATA_DIR = os.getenv("DATA_PATH", os.path.join(PROJECT_ROOT, "data"))

    p = argparse.ArgumentParser()
    p.add_argument("--input", "-i", default=os.path.join(DATA_DIR, "silver", "aircraft_cleaned"), help="Cleaned parquet input dir")
    p.add_argument("--output", "-o", default=os.path.join(DATA_DIR, "gold", "aggregates", "sql"), help="Output dir for query results")
    args = p.parse_args()
    main(args.input, args.output)
