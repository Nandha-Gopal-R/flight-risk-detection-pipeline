from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, hour
import os

# --------------------------------------------------
# Spark Session
# --------------------------------------------------
spark = SparkSession.builder \
    .appName("Aircraft Daily Aggregations") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --------------------------------------------------
# Paths (project-aware)
# --------------------------------------------------
BASE_DIR = os.getenv("PROJECT_ROOT", os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
DATA_DIR = os.getenv("DATA_PATH", os.path.join(BASE_DIR, "app", "output"))

INPUT_PATH = os.path.join(DATA_DIR, "silver", "aircraft_cleaned")
OUTPUT_BASE = os.path.join(DATA_DIR, "gold", "aggregates", "daily")

# --------------------------------------------------
# Read Cleaned Data
# --------------------------------------------------
print("✅ Reading cleaned aircraft data...")
df = spark.read.parquet(INPUT_PATH)

print(f"✅ Total records: {df.count()}")

# --------------------------------------------------
# 1️⃣ Flights per Day
# --------------------------------------------------
print("✅ Computing flights per day...")

flights_per_day = df.groupBy("date") \
    .agg(count("*").alias("total_flights"))

flights_per_day.write \
    .mode("overwrite") \
    .partitionBy("date") \
    .parquet(f"{OUTPUT_BASE}/flights_per_day")

# --------------------------------------------------
# 2️⃣ Unique Aircraft per Day
# --------------------------------------------------
print("✅ Computing unique aircraft per day...")

aircraft_per_day = df.groupBy("date") \
    .agg(countDistinct("icao24").alias("unique_aircraft"))

aircraft_per_day.write \
    .mode("overwrite") \
    .partitionBy("date") \
    .parquet(f"{OUTPUT_BASE}/aircraft_per_day")

# --------------------------------------------------
# 3️⃣ Flights per Hour per Day
# --------------------------------------------------
print("✅ Computing flights per hour...")

flights_per_hour = df \
    .withColumn("hour", hour(col("timestamp"))) \
    .groupBy("date", "hour") \
    .agg(count("*").alias("flights"))

flights_per_hour.write \
    .mode("overwrite") \
    .partitionBy("date") \
    .parquet(f"{OUTPUT_BASE}/flights_per_hour")

print("✅ ALL AGGREGATIONS COMPLETED SUCCESSFULLY")

spark.stop()
