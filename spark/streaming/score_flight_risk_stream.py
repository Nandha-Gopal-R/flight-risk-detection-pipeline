#!/usr/bin/env python3
"""
score_flight_risk_stream.py

Structured streaming app:
  Kafka (aircraft_positions) -> lightweight stateful delta_t/movement -> ML model -> outputs to Kafka + Parquet
"""

import argparse
import json
import os
import math
import sys

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegressionModel

# vector_to_array is optional (exists in newer PySpark)
try:
    from pyspark.ml.functions import vector_to_array  # type: ignore
    HAVE_VECTOR_TO_ARRAY = True
except Exception:
    HAVE_VECTOR_TO_ARRAY = False

# Config via env
PROJECT_ROOT = os.getenv(
    "PROJECT_ROOT",
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
)
DATA_DIR = os.getenv("DATA_PATH", os.path.join(PROJECT_ROOT, "data"))
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

def build_spark(app_name="score_flight_risk_stream"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--kafka", default=KAFKA_BROKER, help="Kafka bootstrap servers")
    p.add_argument("--in-topic", default="aircraft_positions", help="Input Kafka topic containing raw ADS-B JSON")
    p.add_argument("--out-topic", default="flight_risk_scores", help="Output Kafka topic for scored JSON messages")
    p.add_argument("--model", default=os.path.join(DATA_DIR, "ml", "models", "flight_risk_model"),
                   help="Path to trained ML model (PipelineModel or LogisticRegressionModel)")
    p.add_argument("--parquet-out", default=os.path.join(DATA_DIR, "gold", "risk_scores", "stream"),
                   help="Parquet output directory for scored records")
    p.add_argument("--checkpoint", default=os.path.join(DATA_DIR, "checkpoints", "scoring"),
                   help="Checkpoint directory for structured streaming")
    p.add_argument("--starting-offsets", dest="starting_offsets", default="latest", choices=["earliest", "latest"],
                   help="Kafka starting offsets (earliest/latest)")
    return p.parse_args()

def haversine_km(lat1, lon1, lat2, lon2):
    # safe haversine: return None if any missing
    if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
        return None
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    c = 2 * math.asin(min(1.0, math.sqrt(a)))
    return R * c

def main():
    args = parse_args()
    spark = build_spark()

    # JSON schema for incoming Kafka value (tolerant)
    raw_schema = T.StructType() \
        .add("icao24", T.StringType()) \
        .add("lat", T.DoubleType()) \
        .add("lon", T.DoubleType()) \
        .add("flight", T.StringType()) \
        .add("reg", T.StringType()) \
        .add("timestamp", T.LongType()) \
        .add("alt_baro", T.DoubleType()) \
        .add("gs", T.DoubleType()) \
        .add("track", T.DoubleType())

    # Read Kafka stream
    df_kafka = (
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", args.kafka)
            .option("subscribe", args.in_topic)
            .option("startingOffsets", args.starting_offsets)
            .option("failOnDataLoss", "false")
            .load()
    )

    # Parse value -> json string, keep kafka timestamp (TIMESTAMP type)
    json_df = df_kafka.selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_ingest_ts")
    parsed = json_df.select(F.from_json(F.col("json_str"), raw_schema).alias("data"), "kafka_ingest_ts")

    # Flatten fields, tolerant fallback: use data.timestamp (epoch seconds) else kafka_ingest_ts as epoch seconds
    processed = parsed.select(
        F.lower(F.coalesce(F.trim(F.col("data.icao24")), F.lit(""))).alias("icao24"),
        F.col("data.lat").alias("latitude"),
        F.col("data.lon").alias("longitude"),
        F.trim(F.when(F.col("data.flight").isNotNull(), F.col("data.flight")).otherwise(F.lit(""))).alias("flight"),
        F.col("data.reg").alias("reg"),
        # If producer provided epoch seconds (data.timestamp) use it (already seconds). Otherwise use kafka_ingest_ts casted to long seconds.
        F.when(F.col("data.timestamp").isNotNull(), F.col("data.timestamp").cast("long")) \
         .otherwise(F.col("kafka_ingest_ts").cast("long")).alias("evt_ts"),
        F.col("data.alt_baro").alias("alt_baro"),
        F.col("data.gs").alias("gs"),
        F.col("data.track").alias("track")
    )

    # Prepare directories & state store path
    os.makedirs(os.path.dirname(args.parquet_out), exist_ok=True)
    os.makedirs(os.path.dirname(args.checkpoint), exist_ok=True)
    state_store_path = os.path.join(DATA_DIR, "state", "flight_risk_state.json")
    os.makedirs(os.path.dirname(state_store_path), exist_ok=True)

    # load model (PipelineModel preferred, fallback to LogisticRegressionModel)
    model = None
    try:
        from pyspark.ml import PipelineModel
        try:
            model = PipelineModel.load(args.model)
        except Exception:
            # fallback
            model = LogisticRegressionModel.load(args.model)
    except Exception:
        # if PipelineModel not available, try direct LR model
        model = LogisticRegressionModel.load(args.model)

    # ensure model loaded
    if model is None:
        raise RuntimeError(f"Unable to load model from {args.model}")

    # assembler: features expected by the model
    assembler = VectorAssembler(inputCols=["delta_t_double", "movement_score"], outputCol="features", handleInvalid="keep")

    # helper state load/save
    def load_state():
        try:
            with open(state_store_path, "r") as f:
                return json.load(f)
        except Exception:
            return {}

    def save_state(state):
        tmp = state_store_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(state, f)
        os.replace(tmp, state_store_path)

    # foreachBatch handler
    def process_batch(batch_df, batch_id):
        # batch_df: spark DataFrame for this microbatch
        # quickly skip empty batches
        if batch_df.rdd.isEmpty():
            return

        # bring minimal columns to driver as pandas DataFrame
        needed = batch_df.select("icao24", "evt_ts", "latitude", "longitude").where(F.col("icao24") != "")

        try:
            pdf = needed.toPandas()
        except Exception:
            rows = needed.collect()
            pdf = None
            if rows:
                l = []
                for r in rows:
                    l.append({
                        "icao24": r["icao24"],
                        "evt_ts": r["evt_ts"],
                        "latitude": r["latitude"],
                        "longitude": r["longitude"]
                    })
                pdf = l
            else:
                return

        # Load persistent state
        state = load_state()  # {icao: {"last_ts": int, "lat": float, "lon": float}}

        out_rows = []

        # support both pandas DataFrame and list-of-dicts
        if isinstance(pdf, list):
            iterable = pdf
        else:
            iterable = pdf.to_dict(orient="records")

        for rec in iterable:
            icao = (rec.get("icao24") or "").strip()
            if not icao:
                continue
            ts = rec.get("evt_ts")
            try:
                ts = int(ts) if ts is not None else None
            except Exception:
                ts = None
            lat = rec.get("latitude")
            lon = rec.get("longitude")

            s = state.get(icao)
            if s:
                last_ts = s.get("last_ts")
                last_lat = s.get("lat")
                last_lon = s.get("lon")
                delta_t = None
                if last_ts is not None and ts is not None:
                    delta_t = max(0, ts - int(last_ts))
                # compute movement_km and movement_score = km / max(delta_t,1)
                km = haversine_km(last_lat, last_lon, lat, lon) if last_lat is not None and last_lon is not None and lat is not None and lon is not None else None
                mv_score = None
                if km is not None and delta_t not in (None, 0):
                    mv_score = km / max(1.0, float(delta_t))
                out_rows.append({
                    "icao24": icao,
                    "delta_t": int(delta_t) if delta_t is not None else None,
                    "movement_score": float(mv_score) if mv_score is not None else 0.0,
                    "latitude": float(lat) if lat is not None else None,
                    "longitude": float(lon) if lon is not None else None,
                    "evt_ts": ts
                })
            else:
                # first observation for this icao
                out_rows.append({
                    "icao24": icao,
                    "delta_t": None,
                    "movement_score": 0.0,
                    "latitude": float(lat) if lat is not None else None,
                    "longitude": float(lon) if lon is not None else None,
                    "evt_ts": ts
                })

            # update state entry
            state[icao] = {
                "last_ts": int(ts) if ts is not None else (s.get("last_ts") if s else None),
                "lat": float(lat) if lat is not None else (s.get("lat") if s else None),
                "lon": float(lon) if lon is not None else (s.get("lon") if s else None)
            }

        # persist state
        save_state(state)

        # create Spark DataFrame from out_rows
        if not out_rows:
            return
        scored_pdf = spark.createDataFrame(out_rows)

        # -- NULL-SAFE: coalesce missing feature values to safe defaults --
        scored_pdf = scored_pdf.withColumn("delta_t", F.coalesce(F.col("delta_t"), F.lit(0)).cast("long")) \
                               .withColumn("movement_score", F.coalesce(F.col("movement_score"), F.lit(0.0)).cast("double"))

        # cast types / assemble features
        scored_pdf = scored_pdf.withColumn("delta_t_double", F.col("delta_t").cast("double"))
        assembled = assembler.transform(scored_pdf)

        # model transform (model could be LRModel or PipelineModel)
        scored = model.transform(assembled)

        # extract probability for positive class (index 1)
        if HAVE_VECTOR_TO_ARRAY:
            scored = scored.withColumn("prob_array", vector_to_array(F.col("probability")))
            scored = scored.withColumn("risk_probability", F.col("prob_array").getItem(1))
        else:
            # fallback udf
            def _prob1(v):
                try:
                    return float(v[1])
                except Exception:
                    return None
            prob1_udf = F.udf(_prob1, T.DoubleType())
            scored = scored.withColumn("risk_probability", prob1_udf(F.col("probability")))

        scored = scored.withColumn("predicted_risk", F.col("prediction").cast("int"))

        # select output
        out = scored.select("icao24", "evt_ts", "delta_t", "movement_score", "risk_probability", "predicted_risk", "latitude", "longitude")

        # write parquet (append)
        out.write.mode("append").parquet(args.parquet_out)

        # write to Kafka as JSON messages (batch write)
        json_col = F.to_json(F.struct(*[F.col(c) for c in out.columns])).alias("value")
        kafka_df = out.select(json_col)
        kafka_df.selectExpr("CAST(value AS STRING) AS value") \
            .write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", args.kafka) \
            .option("topic", args.out_topic) \
            .save()

    # Build a small stream DF with only required columns and drop empty icao
    stream_df = processed.select("icao24", "evt_ts", "latitude", "longitude").where(F.col("icao24") != "")

    # Start streaming using foreachBatch
    query = (
        stream_df.writeStream
        .outputMode("append")
        .foreachBatch(process_batch)
        .option("checkpointLocation", args.checkpoint)
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("Started streaming flight risk scorer. Writing to Kafka topic:", args.out_topic)
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Interrupted; stopping stream")
        query.stop()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
