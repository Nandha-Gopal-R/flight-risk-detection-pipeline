#!/usr/bin/env bash
# stop_streaming_only.sh
# Stops all services started by start_streaming_only.sh
set -euo pipefail

# project-aware base dir (override with PROJECT_ROOT)
BASE="${PROJECT_ROOT:-${HOME}/bigdata}"
RUN="${BASE}/run"
LOGS="${BASE}/logs"

stop_pidfile(){
  local pf="$1"
  local name="$2"

  if [ -f "$pf" ]; then
    local pid
    pid="$(cat "$pf" 2>/dev/null || true)"

    if [ -n "${pid}" ] && kill -0 "${pid}" 2>/dev/null; then
      echo "Stopping ${name} (pid ${pid})..."
      kill "${pid}" 2>/dev/null || true
      sleep 1

      # force kill if still alive
      if kill -0 "${pid}" 2>/dev/null; then
        echo "Force killing ${name} (pid ${pid})..."
        kill -9 "${pid}" 2>/dev/null || true
      fi
    else
      echo "${name}: pidfile exists but process not running"
    fi

    rm -f "$pf"
  else
    echo "${name}: not running (no pidfile)"
  fi
}

echo "Stopping streaming services using pidfiles in ${RUN}"
echo "--------------------------------------------------"

# 🔴 ORDER MATTERS
# Stop data producers and Spark jobs first, infra last

stop_pidfile "${RUN}/producer.pid"        "ADS-B Producer"
stop_pidfile "${RUN}/spark_score.pid"     "Spark Scoring Stream"
stop_pidfile "${RUN}/spark_parquet.pid"   "Spark Bronze Streaming (Kafka → Parquet)"

stop_pidfile "${RUN}/dashboard.pid"        "Streamlit Dashboard"

stop_pidfile "${RUN}/kafka.pid"            "Kafka Broker"
stop_pidfile "${RUN}/zookeeper.pid"        "ZooKeeper"

echo "--------------------------------------------------"
echo "All streaming services stopped."
echo "Logs preserved in ${LOGS}"
