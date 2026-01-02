#!/usr/bin/env python3
"""
train_flight_risk_model.py
Train ML model for flight risk prediction
"""

import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import when
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator


def spark_session():
    return (
        SparkSession.builder
        .appName("TrainFlightRiskModel")
        .master("local[*]")
        .getOrCreate()
    )


def main(features_path, model_out):
    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f" Loading ML features from {features_path}")
    df = spark.read.parquet(features_path)

    # -------------------------------------------------
    #  CREATE LABEL (this was missing)
    # Simple rule-based label for demo / training
    # -------------------------------------------------
    df = df.withColumn(
        "risk_label",
        when(df.movement_score >= 0.7, 1).otherwise(0)
    )

    # Assemble features
    assembler = VectorAssembler(
        inputCols=["delta_t", "movement_score"],
        outputCol="features"
    )

    data = assembler.transform(df).select("features", "risk_label")

    # Train / Test split
    train_df, test_df = data.randomSplit([0.8, 0.2], seed=42)

    print(" Training Logistic Regression model")

    lr = LogisticRegression(
        labelCol="risk_label",
        featuresCol="features",
        maxIter=20
    )

    model = lr.fit(train_df)

    # Evaluate
    predictions = model.transform(test_df)

    evaluator = BinaryClassificationEvaluator(
        labelCol="risk_label",
        metricName="areaUnderROC"
    )

    auc = evaluator.evaluate(predictions)
    print(f"✅ Model AUC = {auc:.4f}")

    # Save model
    print(f"✅ Saving model to {model_out}")
    model.write().overwrite().save(model_out)

    print("✅ Training completed successfully")
    spark.stop()


if __name__ == "__main__":
    # compute sane project-relative defaults (env override allowed)
    BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    DATA_DIR = os.getenv("DATA_PATH", os.path.join(BASE_DIR, "data"))
    default_features = os.path.join(DATA_DIR, "ml", "training_data", "flight_features")
    default_model_out = os.path.join(DATA_DIR, "ml", "models", "flight_risk_model")

    parser = argparse.ArgumentParser(description="Train Flight Risk ML Model")
    parser.add_argument(
        "--features",
        default=default_features,
        required=True,
        help="Path to aircraft_ml_features"
    )
    parser.add_argument(
        "--model-out",
        default=default_model_out,
        required=True,
        help="Output path for trained model"
    )

    args = parser.parse_args()
    main(args.features, args.model_out)
