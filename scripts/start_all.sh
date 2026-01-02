#!/usr/bin/env bash
# Starts: ZooKeeper, Kafka, Spark bronze streaming (Kafka->bronze parquet),
# scoring stream (Kafka -> scorer -> parquet+Kafka), ADS-B producer, optional dashboard.
# Does NOT run cleaning, feature, promote, or aggregate jobs.
set -euo pipefail

# project-aware base directory (override with env PROJECT_ROOT)
BASE="${PROJECT_ROOT:-${HOME}/bigdata}"
KAFKA_DIR="${BASE}/kafka"
SPARK_DIR="${BASE}/spark"
APP_DIR="${BASE}/app"
SCRIPTS_DIR="${BASE}/scripts"
VENV_PY="${BASE}/venv/bin/python3"          # optional venv python
LOGS="${BASE}/logs"
RUN="${BASE}/run"

ZK_BIN="${KAFKA_DIR}/bin/zookeeper-server-start.sh"
ZK_CONFIG="${KAFKA_DIR}/config/zookeeper.properties"
KAFKA_BIN="${KAFKA_DIR}/bin/kafka-server-start.sh"
KAFKA_TOPICS="${KAFKA_DIR}/bin/kafka-topics.sh"

SPARK_SUBMIT="${SPARK_DIR}/bin/spark-submit"

# apps
SPARK_STREAM_APP="${APP_DIR}/spark/spark_aircraft_parquet.py"      # kafka -> bronze
SCORE_STREAM_APP="${APP_DIR}/spark/score_flight_risk_stream.py"    # scoring stream
PRODUCER="${APP_DIR}/producer/adsb_producer.py"
DASHBOARD="${APP_DIR}/dashboard/flight_dashboard.py"

VALIDATOR_SCRIPT="${SCRIPTS_DIR}/validate.sh"  # unused here (kept for reference)

# Kafka / topics
TOPIC="aircraft_positions"
OUT_TOPIC="flight_risk_scores"
PARTITIONS=3
REPLICATION=1

# Canonical outputs (only those used by streaming)
BRONZE_OUT="${BASE}/app/output/bronze/aircraft_positions"
CHECKPOINT_BRONZE="${BASE}/app/output/checkpoints/bronze"

# scoring stream outputs / model
SCORING_PARQUET_OUT="${BASE}/app/output/gold/risk_scores/stream"
MODEL_PATH="${BASE}/app/output/ml/models/flight_risk_model"
CHECKPOINT_SCORING="${BASE}/app/output/checkpoints/scoring"

# make directories for logs / pid files / outputs
mkdir -p "${LOGS}" "${RUN}" "${BASE}/app/output" "${BRONZE_OUT}" "${CHECKPOINT_BRONZE}" "${SCORING_PARQUET_OUT}" "${CHECKPOINT_SCORING}"

log(){ printf "[%s] %s\n" "$(date '+%F %T')" "$*"; }

is_running_pidfile(){
  local pf="$1"
  [ -f "$pf" ] || return 1
  local pid
  pid="$(cat "$pf" 2>/dev/null || echo "")"
  [ -z "$pid" ] && return 1
  kill -0 "$pid" 2>/dev/null
}

port_is_open(){
  local host="$1" port="$2"
  (echo > /dev/tcp/"${host}"/"${port}") >/dev/null 2>&1
  return $?
}

# --- Start ZooKeeper ---
ZK_PID="${RUN}/zookeeper.pid"; ZK_LOG="${LOGS}/zookeeper.log"
if is_running_pidfile "${ZK_PID}"; then
  log "ZooKeeper already running (pidfile ${ZK_PID})"
else
  log "Starting ZooKeeper..."
  nohup "${ZK_BIN}" "${ZK_CONFIG}" > "${ZK_LOG}" 2>&1 &
  echo $! > "${ZK_PID}"
  sleep 1
fi

log "Waiting for ZooKeeper (2181)..."
for i in {1..30}; do
  if port_is_open "localhost" 2181; then log "ZooKeeper up"; break; fi
  sleep 1
done

# --- Start Kafka ---
KAFKA_PID="${RUN}/kafka.pid"; KAFKA_LOG="${LOGS}/kafka.log"
if is_running_pidfile "${KAFKA_PID}"; then
  log "Kafka already running (pidfile ${KAFKA_PID})"
else
  log "Starting Kafka..."
  nohup "${KAFKA_BIN}" "${KAFKA_DIR}/config/server.properties" > "${KAFKA_LOG}" 2>&1 &
  echo $! > "${KAFKA_PID}"
  sleep 1
fi

log "Waiting for Kafka port 9092..."
KAFKA_READY=false
for i in {1..60}; do
  if port_is_open "localhost" 9092; then
    log "Kafka ready (port 9092 open)"
    KAFKA_READY=true
    break
  fi
  sleep 1
done

if [ "${KAFKA_READY}" = false ]; then
  log "Kafka port never opened. Check ${KAFKA_LOG}"
  exit 1
fi


# Ensure topic exists (idempotent)
log "Ensuring topic ${TOPIC} exists..."
"${KAFKA_TOPICS}" --bootstrap-server localhost:9092 --create --topic "${TOPIC}" \
  --partitions "${PARTITIONS}" --replication-factor "${REPLICATION}" > "${LOGS}/kafka_topic_create.log" 2>&1 || true

# --- Start bronze streaming (Kafka -> Parquet) ---
SPARK_BRONZE_PID="${RUN}/spark_parquet.pid"; SPARK_BRONZE_LOG="${LOGS}/spark_parquet.log"
if is_running_pidfile "${SPARK_BRONZE_PID}"; then
  log "Spark bronze streaming already running (pidfile ${SPARK_BRONZE_PID})"
else
  log "Starting Spark parquet streaming job (kafka -> bronze parquet)..."
  if [ -x "${VENV_PY}" ]; then
    export PYSPARK_PYTHON="${VENV_PY}"
    export PYSPARK_DRIVER_PYTHON="${VENV_PY}"
  fi
  nohup "${SPARK_SUBMIT}" --master local[*] \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
    "${SPARK_STREAM_APP}" \
      --kafka localhost:9092 --topic "${TOPIC}" \
      --output "${BRONZE_OUT}" --checkpoint "${CHECKPOINT_BRONZE}" \
    > "${SPARK_BRONZE_LOG}" 2>&1 &
  echo $! > "${SPARK_BRONZE_PID}"
  sleep 3
fi

# --- Start scoring stream (Kafka -> scorer -> kafka + parquet) ---
SCORE_PID="${RUN}/spark_score.pid"; SCORE_LOG="${LOGS}/spark_score.log"
if is_running_pidfile "${SCORE_PID}"; then
  log "Scoring stream already running (pidfile ${SCORE_PID})"
else
  if [ -f "${SCORE_STREAM_APP}" ]; then
    log "Starting scoring stream (Kafka -> scoring) ..."
    if [ -x "${VENV_PY}" ]; then
      export PYSPARK_PYTHON="${VENV_PY}"
      export PYSPARK_DRIVER_PYTHON="${VENV_PY}"
    fi
    nohup "${SPARK_SUBMIT}" --master local[*] \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
      "${SCORE_STREAM_APP}" --kafka localhost:9092 --in-topic "${TOPIC}" --out-topic "${OUT_TOPIC}" \
      --model "${MODEL_PATH}" --parquet-out "${SCORING_PARQUET_OUT}" \
      --checkpoint "${CHECKPOINT_SCORING}" --starting-offsets latest \
      > "${SCORE_LOG}" 2>&1 &
    echo $! > "${SCORE_PID}"
    sleep 2
  else
    log "Score stream app missing; skipping scoring"
  fi
fi

# --- Start ADS-B producer (after Kafka + streaming started) ---
PROD_PID="${RUN}/producer.pid"; PROD_LOG="${LOGS}/producer.log"
if is_running_pidfile "${PROD_PID}"; then
  log "Producer already running (pidfile ${PROD_PID})"
else
  log "Starting ADS-B producer..."
  if [ -x "${VENV_PY}" ]; then
    nohup "${VENV_PY}" "${PRODUCER}" > "${PROD_LOG}" 2>&1 &
  else
    nohup python3 "${PRODUCER}" > "${PROD_LOG}" 2>&1 &
  fi
  echo $! > "${PROD_PID}"
  sleep 1
fi

# --- Optional Streamlit dashboard ---
DASH_PID="${RUN}/dashboard.pid"; DASH_LOG="${LOGS}/dashboard.log"
STREAMLIT_BIN="${BASE}/venv/bin/streamlit"
if [ -x "${STREAMLIT_BIN}" ] && [ -f "${DASHBOARD}" ]; then
  if is_running_pidfile "${DASH_PID}"; then
    log "Dashboard already running"
  else
    log "Starting dashboard (localhost:8501)..."
    nohup "${STREAMLIT_BIN}" run "${DASHBOARD}" --server.port 8501 --server.address localhost > "${DASH_LOG}" 2>&1 &
    echo $! > "${DASH_PID}"
  fi
else
  log "Streamlit not available or dashboard missing; skipping dashboard"
fi

log "Startup sequence finished. Check logs in ${LOGS} for details."
