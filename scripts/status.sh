#!/usr/bin/env bash
# status.sh - improved summary of running pipeline processes and log tails
# Usage: ./status.sh
set -euo pipefail

# project-aware base dir (override with PROJECT_ROOT)
BASE="${PROJECT_ROOT:-${HOME}/bigdata}"
RUN="${BASE}/run"
LOGS="${BASE}/logs"
KAFKA_BIN="${BASE}/kafka/bin/kafka-topics.sh"

# List of services we expect pidfiles for (order matters for display)
SERVICES=(producer spark_parquet spark_score kafka zookeeper dashboard)

# number of log lines to show per file
TAIL_LINES=50

# helper: print a header
hdr() {
  printf "\n=== %s ===\n" "$1"
}

# show pidfiles directory
hdr "PID files in ${RUN}:"
if [ -d "${RUN}" ]; then
  ls -l "${RUN}" || true
else
  echo "(run dir not found: ${RUN})"
fi

# check each expected service
hdr "Service status (pidfile -> running? -> process info)"
for s in "${SERVICES[@]}"; do
  pf="${RUN}/${s}.pid"
  if [ -f "${pf}" ]; then
    pid="$(cat "${pf}" 2>/dev/null || echo "")"
    if [ -z "${pid}" ]; then
      echo "${s}: pidfile exists but empty (${pf})"
      continue
    fi

    # check process exists
    if kill -0 "${pid}" 2>/dev/null; then
      # get process command/args (if ps available)
      if command -v ps >/dev/null 2>&1; then
        cmd="$(ps -p "${pid}" -o args= 2>/dev/null || echo "ps:unknown")"
      else
        cmd="(ps not available)"
      fi
      printf "%-15s: pid %-7s -> running -> %s\n" "${s}" "${pid}" "${cmd}"
    else
      printf "%-15s: pid %-7s -> NOT running (stale pidfile?)\n" "${s}" "${pid}"
    fi
  else
    printf "%-15s: not running (no pidfile: %s)\n" "${s}" "${pf}"
  fi
done

# show listening ports for common services (ss or netstat)
hdr "Network listeners (searching ports 2181, 9092, 50070, 4040, 8501 etc.)"
PORTS=(2181 9092 4040 4041 8080 8501 50070)
if command -v ss >/dev/null 2>&1; then
  for p in "${PORTS[@]}"; do
    ss -ltnp "( sport = :${p} )" 2>/dev/null | sed -n "1,2p" || true
  done
elif command -v netstat >/dev/null 2>&1; then
  for p in "${PORTS[@]}"; do
    netstat -ltnp 2>/dev/null | grep -E ":${p}[[:space:]]" || true
  done
else
  echo "ss/netstat not available; skipping port checks."
fi

# optional: try to list Kafka topics if kafka-topics script exists and Kafka appears ready
if [ -x "${KAFKA_BIN}" ]; then
  hdr "Kafka topics (using ${KAFKA_BIN})"
  if timeout 2 "${KAFKA_BIN}" --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
    # print actual topics (short)
    "${KAFKA_BIN}" --bootstrap-server localhost:9092 --list 2>/dev/null || echo "(no topics or error)"
  else
    echo "Kafka tool present but could not contact localhost:9092 (broker may be down)"
  fi
else
  hdr "Kafka topics"
  echo "kafka-topics.sh not found at ${KAFKA_BIN}; skipping topic list."
fi

# Tail logs
hdr "Last ${TAIL_LINES} lines from logs"
for logf in producer.log spark_parquet.log spark_score.log kafka.log zookeeper.log dashboard.log; do
  logfile="${LOGS}/${logf}"
  echo
  echo "---- ${logfile} ----"
  if [ -f "${logfile}" ]; then
    # show a small header and tail
    printf "++ %s (size: %s) ++\n" "${logfile}" "$(stat -c%s "${logfile}" 2>/dev/null || echo "unknown")"
    tail -n "${TAIL_LINES}" "${logfile}" || true
  else
    echo "(no log file)"
  fi
done

hdr "Summary"
echo " - Check 'kafka' and 'zookeeper' entries above for actual running processes."
echo " - If pidfile exists but process not running, consider removing stale pidfile and restarting the service."
echo " - For Kafka topic issues, inspect ${LOGS}/kafka.log"
