#!/usr/bin/env bash
# clean_all.sh  - CAREFUL: will remove output and checkpoints
set -euo pipefail

# Use PROJECT_ROOT env if provided, else default to $HOME/aircraft
BASE="${PROJECT_ROOT:-${HOME}/aircraft}"
OUT="${BASE}/app/output"
RUN="${BASE}/run"
LOGS="${BASE}/logs"

echo "Stopping running services first..."
"${BASE}/app/scripts/stop_all.sh" || true

echo "Removing parquet outputs, checkpoints and state (this is destructive)..."
rm -rf "${OUT}/aircraft_parquet" \
       "${OUT}/aircraft_cleaned" \
       "${OUT}/aircraft_gold" \
       "${OUT}/aircraft_aggregated"* \
       "${OUT}/aircraft_aggregated_sql" \
       "${OUT}/aircraft_ml_features" \
       "${OUT}/flight_risk_stream" \
       "${OUT}/flight_risk_scores" \
       "${OUT}/ml_models" \
       "${OUT}/checkpoints_parquet" \
       "${OUT}/checkpoints_score_stream" \
       "${OUT}/data_quality" \
       "${OUT}/_tmp"* || true

echo "Removing streaming state file..."
rm -f "${OUT}/flight_risk_state.json" || true

echo "Done. Logs are preserved in ${LOGS}."
