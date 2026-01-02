#!/usr/bin/env python3
import argparse
from pyspark.sql import SparkSession, functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.functions import vector_to_array

def main(args):
    spark = SparkSession.builder \
        .appName("score_flight_risk") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print(f"✅ Reading ML features from: {args.features}")
    df = spark.read.parquet(args.features)

    required = ["icao24", "delta_t", "movement_score"]
    for c in required:
        if c not in df.columns:
            raise ValueError(f"Missing column: {c}")

    assembler = VectorAssembler(
        inputCols=["delta_t", "movement_score"],
        outputCol="features"
    )

    df_feat = assembler.transform(df)

    print(f"✅ Loading LogisticRegression model from: {args.model}")
    model = LogisticRegressionModel.load(args.model)

    scored = model.transform(df_feat)

    scored = scored.withColumn(
        "prob_array", vector_to_array("probability")
    )

    result = scored.select(
        "icao24",
        "delta_t",
        "movement_score",
        F.col("prediction").cast("int").alias("predicted_risk"),
        F.col("prob_array")[1].alias("risk_probability")
    )

    print(f"✅ Writing flight risk scores to: {args.out}")
    result.write.mode("overwrite").parquet(args.out)

    print("✅ Live risk scoring completed successfully")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--features", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    main(args)
