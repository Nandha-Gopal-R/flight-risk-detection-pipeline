#!/usr/bin/env python3
"""
spark_aircraft_parquet.py

Read aircraft messages from Kafka (JSON), parse and write streaming Parquet
partitioned by date (bronze layer).

Usage:
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
      spark_aircraft_parquet.py \
      --kafka localhost:9092 --topic aircraft_positions \
      --output /home/nandha/bigdata/app/output/bronze/aircraft_positions \
      --checkpoint /home/nandha/bigdata/app/output/checkpoints/bronze
"""
import argparse
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, when, lit, lower, from_unixtime, to_date, current_timestamp
)
from pyspark.sql.types import StructType, StringType, DoubleType, LongType

# env-config
PROJECT_ROOT = os.getenv(
    "PROJECT_ROOT",
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
)
DATA_DIR = os.getenv("DATA_PATH", os.path.join(PROJECT_ROOT, "data"))
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

def build_spark(app_name="AircraftKafkaToParquet"):
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .getOrCreate()

def get_schema():
    # Schema expects timestamp as epoch seconds (Long); tolerant of missing fields
    return StructType() \
        .add("icao24", StringType()) \
        .add("lat", DoubleType()) \
        .add("lon", DoubleType()) \
        .add("flight", StringType()) \
        .add("reg", StringType()) \
        .add("timestamp", LongType())

def main(args):
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    KAFKA_SERVER = args.kafka
    TOPIC = args.topic
    OUT_PATH = args.output
    CHECKPOINT = args.checkpoint
    STARTING = args.starting_offsets
    TRIGGER = args.trigger

    schema = get_schema()

    # Read stream from Kafka (value as string). Keep kafka ingestion timestamp as fallback.
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVER)
        .option("subscribe", TOPIC)
        .option("startingOffsets", STARTING)        # "earliest" or "latest"
        .option("failOnDataLoss", "false")         # don't crash if Kafka offsets were lost
        .load()
    )

    # keep kafka ingestion timestamp as kafka_ts; cast value -> string
    json_df = kafka_df.selectExpr("CAST(value AS STRING) AS value", "timestamp AS kafka_ts")

    # parse the JSON payload into a struct 'data'
    parsed = json_df.select(from_json(col("value"), schema).alias("data"), col("kafka_ts"))

    # flatten fields and compute a proper event timestamp:
    # if producer provided epoch seconds (data.timestamp) use it -> from_unixtime(...)
    # otherwise use kafka ingestion timestamp (kafka_ts)
    flattened = parsed.select(
        when(
            (col("data.icao24").isNotNull()) & (col("data.icao24") != ""),
            lower(col("data.icao24"))
        ).otherwise(
            when((col("data.flight").isNotNull()) & (col("data.flight") != ""),
                 lower(col("data.flight")))
            .otherwise(
                when((col("data.reg").isNotNull()) & (col("data.reg") != ""),
                     lower(col("data.reg")))
                .otherwise(lit("unknown"))
            )
        ).alias("icao24"),
        col("data.lat").alias("latitude"),
        col("data.lon").alias("longitude"),
        col("data.flight").alias("flight"),
        col("data.reg").alias("reg"),
        # event timestamp: prefer data.timestamp (epoch seconds) else kafka ingestion ts
        when(col("data.timestamp").isNotNull(),
             from_unixtime(col("data.timestamp")).cast("timestamp")
        ).otherwise(col("kafka_ts")).alias("timestamp")
    )

    # ensure timestamp exists; fallback to current time if kafka_ts was also null (very unlikely)
    flattened = flattened.withColumn("timestamp", when(col("timestamp").isNull(), current_timestamp()).otherwise(col("timestamp")))

    # derive date column for partitioning
    flattened = flattened.withColumn("date", to_date(col("timestamp")))

    # Add watermark and deduplicate using event-time (timestamp). This prevents duplicates on restart.
    # Watermark must reference the same column used for dropDuplicates event-time.
    deduped = flattened.withWatermark("timestamp", "10 minutes").dropDuplicates(["icao24", "timestamp"])

    # Optional: reduce small files by coalescing per micro-batch (adjust number to match your setup)
    # deduped = deduped.repartition(4)

    # Write streaming parquet, partitioned by date, with Snappy compression
    query = (
        deduped.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", OUT_PATH)
        .option("checkpointLocation", CHECKPOINT)
        .option("compression", "snappy")
        .partitionBy("date")
        .trigger(processingTime=TRIGGER)
        .start()
    )

    print(f"Started streaming query writing to {OUT_PATH} (checkpoint={CHECKPOINT})")
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Interrupted by user - stopping query...")
        query.stop()
    except Exception as ex:
        # If offsets were lost you'll see exceptions; we set failOnDataLoss=false to avoid crash,
        # but keep this here to show any other runtime failures.
        print("Streaming query failed:", ex, file=sys.stderr)
        try:
            query.stop()
        except Exception:
            pass
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    PROJECT_ROOT = os.getenv(
        "PROJECT_ROOT",
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    )
    DATA_DIR = os.getenv("DATA_PATH", os.path.join(PROJECT_ROOT, "data"))
    KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

    p = argparse.ArgumentParser(description="Spark streaming: Kafka -> Parquet")
    p.add_argument("--kafka", default=KAFKA_BROKER, help="Kafka bootstrap servers")
    p.add_argument("--topic", default="aircraft_positions", help="Kafka topic to subscribe")
    p.add_argument("--output", default=os.path.join(DATA_DIR, "bronze", "aircraft_positions"),
                   help="Output parquet directory")
    p.add_argument("--checkpoint", default=os.path.join(DATA_DIR, "checkpoints", "bronze"),
                   help="Checkpoint directory for structured streaming")
    p.add_argument("--starting-offsets", dest="starting_offsets", default="earliest",
                   choices=["earliest", "latest"],
                   help="Starting offsets for Kafka source (earliest/latest)")
    p.add_argument("--trigger", default="10 seconds", help="Micro-batch trigger interval")
    args = p.parse_args()
    main(args)
