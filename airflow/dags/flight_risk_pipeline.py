from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

#  PATH CONFIG 
PROJECT_ROOT = os.getenv(
    "PROJECT_ROOT",
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
)

SPARK_SUBMIT = os.getenv("SPARK_SUBMIT", "spark-submit")
VENV_ACTIVATE = os.getenv("VENV_ACTIVATE", "venv/bin/activate")

DATA_DIR = os.getenv("DATA_PATH", os.path.join(PROJECT_ROOT, "data"))
LOG_DIR = os.path.join(PROJECT_ROOT, "logs", "batch_jobs")

SPARK_DIR = os.path.join(PROJECT_ROOT, "spark")
ML_DIR = os.path.join(PROJECT_ROOT, "ml")

CLEAN_APP = os.path.join(SPARK_DIR, "batch", "clean_aircraft_parquet.py")
VALIDATE_APP = os.path.join(SPARK_DIR, "batch", "validate_aircraft_data.py")
AGG_APP = os.path.join(SPARK_DIR, "batch", "aggregate_aircraft_daily.py")
SQL_APP = os.path.join(SPARK_DIR, "batch", "spark_sql_analytics.py")
FEATURE_APP = os.path.join(ML_DIR, "feature_engineer_ml.py")
BUILD_APP = os.path.join(ML_DIR, "build_features_from_gold.py")
TRAIN_APP = os.path.join(ML_DIR, "train_flight_risk_model.py")

# ---------------- DEFAULT ARGS ----------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="flight_risk_split_pipeline",
    default_args=default_args,
    description="Flight risk pipeline (clean → validate → agg → sql → feature → build → train)",
    start_date=datetime(2025, 12, 24),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["bigdata", "spark", "ml"],
) as dag:

    spark_cmd = (
        "source {venv} && "
        "export PYSPARK_PYTHON=$(which python) && "
        "export PYSPARK_DRIVER_PYTHON=$(which python) && "
        "{spark} --master local[*] {app} {args} > {log} 2>&1"
    )

    t_clean = BashOperator(
        task_id="clean_bronze_to_silver",
        bash_command=spark_cmd.format(
            venv=VENV_ACTIVATE,
            spark=SPARK_SUBMIT,
            app=CLEAN_APP,
            args=f"--input {DATA_DIR}/bronze/aircraft_positions --output {DATA_DIR}/silver/aircraft_cleaned",
            log=os.path.join(LOG_DIR, "clean.log"),
        ),
    )

    t_validate = BashOperator(
        task_id="validate_silver",
        bash_command=spark_cmd.format(
            venv=VENV_ACTIVATE,
            spark=SPARK_SUBMIT,
            app=VALIDATE_APP,
            args="",
            log=os.path.join(LOG_DIR, "validate.log"),
        ),
    )

    t_agg = BashOperator(
        task_id="aggregations_gold",
        bash_command=spark_cmd.format(
            venv=VENV_ACTIVATE,
            spark=SPARK_SUBMIT,
            app=AGG_APP,
            args="",
            log=os.path.join(LOG_DIR, "agg.log"),
        ),
    )

    t_sql = BashOperator(
        task_id="sql_analytics",
        bash_command=spark_cmd.format(
            venv=VENV_ACTIVATE,
            spark=SPARK_SUBMIT,
            app=SQL_APP,
            args=f"--input {DATA_DIR}/silver/aircraft_cleaned --output {DATA_DIR}/gold/aggregates/sql",
            log=os.path.join(LOG_DIR, "sql_analytics.log"),
        ),
    )

    t_feature = BashOperator(
        task_id="feature_engineer",
        bash_command=spark_cmd.format(
            venv=VENV_ACTIVATE,
            spark=SPARK_SUBMIT,
            app=FEATURE_APP,
            args=f"--input {DATA_DIR}/silver/aircraft_cleaned --out {DATA_DIR}/ml/training_data/flight_features",
            log=os.path.join(LOG_DIR, "feature.log"),
        ),
    )

    t_build = BashOperator(
        task_id="build_features_from_gold",
        bash_command=spark_cmd.format(
            venv=VENV_ACTIVATE,
            spark=SPARK_SUBMIT,
            app=BUILD_APP,
            args=f"--gold {DATA_DIR}/silver/aircraft_cleaned --out {DATA_DIR}/ml/training_data/flight_features",
            log=os.path.join(LOG_DIR, "build_features.log"),
        ),
    )

    t_train = BashOperator(
        task_id="train_model",
        bash_command=spark_cmd.format(
            venv=VENV_ACTIVATE,
            spark=SPARK_SUBMIT,
            app=TRAIN_APP,
            args=f"--features {DATA_DIR}/ml/training_data/flight_features --model-out {DATA_DIR}/ml/models/flight_risk_model",
            log=os.path.join(LOG_DIR, "train.log"),
        ),
    )

    t_clean >> t_validate >> t_agg >> t_sql >> t_feature >> t_build >> t_train
