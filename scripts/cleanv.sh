#!/usr/bin/env bash
# cleanv.sh
# Batch: 0) Bronze existence
#        1) Bronze -> Silver cleaning
#        2) Validation (Spark)
#        3) Aggregations (silver -> gold aggregates)
#        4) SQL analytics (additional aggregates / CSV)
#        5) Feature engineering (silver -> features)
#        6) Build features from gold (optional / overwrites features)
#        7) Train ML model
#
# Partition-aware parquet checks, per-step logs in dedicated folder.
set -uo pipefail   # do not use -e so we can handle step return codes explicitly

# project-aware base directory (override with env PROJECT_ROOT)
BASE="${PROJECT_ROOT:-${HOME}/bigdata}"
SPARK_DIR="${BASE}/spark"
APP_DIR="${BASE}/app"
BATCH_LOG_DIR="${BASE}/logs/batch_jobs"

mkdir -p "${BATCH_LOG_DIR}"

SPARK_SUBMIT="${SPARK_DIR}/bin/spark-submit"

# Apps (existing + new)
CLEAN_APP="${APP_DIR}/spark/clean_aircraft_parquet.py"
VALIDATE_APP="${APP_DIR}/spark/validate_aircraft_data.py"
FEATURE_APP="${APP_DIR}/spark/feature_engineer_ml.py"
TRAIN_APP="${APP_DIR}/spark/train_flight_risk_model.py"

# NEW: three files you provided
AGG_APP="${APP_DIR}/spark/aggregate_aircraft_daily.py"                # first file (daily aggregations)
BUILD_APP="${APP_DIR}/spark/build_features_from_gold.py"     # second file (build features from gold)
SQL_APP="${APP_DIR}/spark/spark_sql_analytics.py"            # third file (SQL analytics)

# Canonical data locations (adjust if you use a different layout)
BRONZE_IN="${BASE}/app/output/bronze/aircraft_positions"
SILVER_OUT="${BASE}/app/output/silver/aircraft_cleaned"
FEATURES_OUT="${BASE}/app/output/ml/training_data/flight_features"
MODEL_OUT="${BASE}/app/output/ml/models/flight_risk_model"

# GOLD outputs
GOLD_AGG_BASE="${BASE}/app/output/gold/aggregates/daily"
SQL_OUT="${BASE}/app/output/gold/aggregates/sql"

# Per-step logs
CLEAN_LOG="${BATCH_LOG_DIR}/clean.log"
VALIDATE_LOG="${BATCH_LOG_DIR}/validate.log"
AGG_LOG="${BATCH_LOG_DIR}/agg.log"
SQL_LOG="${BATCH_LOG_DIR}/sql_analytics.log"
FEATURE_LOG="${BATCH_LOG_DIR}/feature.log"
BUILD_LOG="${BATCH_LOG_DIR}/build_features.log"
TRAIN_LOG="${BATCH_LOG_DIR}/train.log"

log(){
  printf "[%s] %s\n" "$(date '+%F %T')" "$*"
}

# ---- Basic environment checks ----
if [ ! -x "${SPARK_SUBMIT}" ] && [ ! -f "${SPARK_SUBMIT}" ]; then
  log "ERROR: spark-submit not found at ${SPARK_SUBMIT}"
  exit 2
fi

# check required apps exist; AGG/BUILD/SQL are optional but we'll warn if missing
[ -f "${CLEAN_APP}" ] || { log "ERROR: Clean app missing: ${CLEAN_APP}"; exit 2; }
[ -f "${VALIDATE_APP}" ] || { log "ERROR: Validate app missing: ${VALIDATE_APP}"; exit 2; }
[ -f "${FEATURE_APP}" ] || { log "ERROR: Feature app missing: ${FEATURE_APP}"; exit 2; }
[ -f "${TRAIN_APP}" ] || { log "ERROR: Train app missing: ${TRAIN_APP}"; exit 2; }

if [ ! -f "${AGG_APP}" ]; then
  log "WARN: Aggregation app not found: ${AGG_APP} (skipping aggregation step)"
fi
if [ ! -f "${SQL_APP}" ]; then
  log "WARN: SQL analytics app not found: ${SQL_APP} (skipping SQL analytics step)"
fi
if [ ! -f "${BUILD_APP}" ]; then
  log "WARN: Build-features app not found: ${BUILD_APP} (skipping build_features step)"
fi

# ---- 0) Bronze data existence check (partition-aware, recursive) ----
if [ ! -d "${BRONZE_IN}" ]; then
  log "ERROR: Bronze input directory does not exist: ${BRONZE_IN}"
  exit 2
fi

# recursive search for any parquet file under bronze
if ! find "${BRONZE_IN}" -type f \( -iname "*.parquet" -o -iname "*.snappy.parquet" -o -iname "*.parquet.snappy" \) -print -quit | grep -q .; then
  log "ERROR: Bronze input directory exists but contains no parquet files: ${BRONZE_IN}"
  exit 2
fi

log "✅ Bronze data present: ${BRONZE_IN}"

# ---- 1) Clean (bronze -> silver) ----
log "=== STEP 1: Cleaning (bronze -> silver) ==="
# try with explicit args first (backwards compatible), else fallback to no-args
if "${SPARK_SUBMIT}" --master local[*] "${CLEAN_APP}" --input "${BRONZE_IN}" --output "${SILVER_OUT}" > "${CLEAN_LOG}" 2>&1; then
  log "Cleaning finished (with args). See ${CLEAN_LOG}"
else
  log "Cleaning with args failed; retrying without args (see ${CLEAN_LOG})"
  if "${SPARK_SUBMIT}" --master local[*] "${CLEAN_APP}" > "${CLEAN_LOG}" 2>&1; then
    log "Cleaning finished (without args). See ${CLEAN_LOG}"
  else
    CLEAN_RC=$?
    log "ERROR: Cleaning FAILED (exit ${CLEAN_RC}). Tail of ${CLEAN_LOG}:"
    tail -n 200 "${CLEAN_LOG}" || true
    exit ${CLEAN_RC}
  fi
fi

# verify silver output
if [ ! -d "${SILVER_OUT}" ] || ! find "${SILVER_OUT}" -type f \( -iname "*.parquet" -o -iname "*.snappy.parquet" \) -print -quit | grep -q .; then
  log "ERROR: Silver output not found or empty after cleaning: ${SILVER_OUT}"
  log "---- tail ${CLEAN_LOG} ----"
  tail -n 200 "${CLEAN_LOG}" || true
  exit 2
fi
log "✅ Silver output ready: ${SILVER_OUT}"

# ---- 2) Validation ----
log "=== STEP 2: Validation (Spark) ==="
# Always use spark-submit for Spark jobs to get SparkSession/SparkContext
if "${SPARK_SUBMIT}" --master local[*] "${VALIDATE_APP}" > "${VALIDATE_LOG}" 2>&1; then
  VAL_RC=0
else
  VAL_RC=$?
fi

if [ "${VAL_RC}" -eq 2 ]; then
  log "❌ Validation FAILED (exit 2). Check ${VALIDATE_LOG}"
  tail -n 200 "${VALIDATE_LOG}" || true
  exit 2
elif [ "${VAL_RC}" -eq 1 ]; then
  log "⚠️ Validation WARNING (exit 1). Check ${VALIDATE_LOG} and quarantines. Proceeding to next steps."
  # show brief tail for operator
  tail -n 120 "${VALIDATE_LOG}" || true
else
  log "✅ Validation SUCCESS (exit 0)."
fi

# ---- 3) Aggregations (silver -> gold aggregates) ----
if [ -f "${AGG_APP}" ]; then
  log "=== STEP 3: Aggregations (silver -> gold) ==="
  if "${SPARK_SUBMIT}" --master local[*] "${AGG_APP}" > "${AGG_LOG}" 2>&1; then
    log "Aggregation finished. See ${AGG_LOG}"
  else
    AGG_RC=$?
    log "ERROR: Aggregation FAILED (exit ${AGG_RC}). Tail ${AGG_LOG}:"
    tail -n 200 "${AGG_LOG}" || true
    exit ${AGG_RC}
  fi

  # verify gold aggregation output (simple existence check)
  if [ ! -d "${GOLD_AGG_BASE}" ] || ! find "${GOLD_AGG_BASE}" -type f \( -iname "*.parquet" -o -iname "*.snappy.parquet" \) -print -quit | grep -q .; then
    log "ERROR: Aggregation output not found or empty: ${GOLD_AGG_BASE}"
    tail -n 200 "${AGG_LOG}" || true
    exit 2
  fi
  log "✅ Aggregations written to: ${GOLD_AGG_BASE}"
else
  log "Skipping aggregations step (app not present): ${AGG_APP}"
fi

# ---- 4) SQL analytics (optional) ----
if [ -f "${SQL_APP}" ]; then
  log "=== STEP 4: SQL analytics (additional queries) ==="
  if "${SPARK_SUBMIT}" --master local[*] "${SQL_APP}" --input "${SILVER_OUT}" --output "${SQL_OUT}" > "${SQL_LOG}" 2>&1; then
    log "SQL analytics finished. See ${SQL_LOG}"
  else
    SQL_RC=$?
    log "ERROR: SQL analytics FAILED (exit ${SQL_RC}). Tail ${SQL_LOG}:"
    tail -n 200 "${SQL_LOG}" || true
    exit ${SQL_RC}
  fi

  # verify SQL outputs
  if [ ! -d "${SQL_OUT}" ] || ! find "${SQL_OUT}" -type f \( -iname "*.parquet" -o -iname "*.snappy.parquet" -o -iname "*.csv" \) -print -quit | grep -q .; then
    log "WARNING: SQL analytics output seems empty: ${SQL_OUT}"
  else
    log "✅ SQL analytics written to: ${SQL_OUT}"
  fi
else
  log "Skipping SQL analytics (app not present): ${SQL_APP}"
fi

# ---- 5) Feature engineering (silver -> features) ----
log "=== STEP 5: Feature engineering (silver -> features) ==="
FEATURE_RC=0
if "${SPARK_SUBMIT}" --master local[*] "${FEATURE_APP}" --input "${SILVER_OUT}" --out "${FEATURES_OUT}" > "${FEATURE_LOG}" 2>&1; then
  FEATURE_RC=0
  log "Feature job finished (with --input/--out). See ${FEATURE_LOG}"
elif "${SPARK_SUBMIT}" --master local[*] "${FEATURE_APP}" --gold "${SILVER_OUT}" --out "${FEATURES_OUT}" > "${FEATURE_LOG}" 2>&1; then
  FEATURE_RC=0
  log "Feature job finished (with --gold/--out). See ${FEATURE_LOG}"
elif "${SPARK_SUBMIT}" --master local[*] "${FEATURE_APP}" > "${FEATURE_LOG}" 2>&1; then
  FEATURE_RC=0
  log "Feature job finished (no-args). See ${FEATURE_LOG}"
else
  FEATURE_RC=$?
  log "ERROR: Feature job failed (exit ${FEATURE_RC}). Tail ${FEATURE_LOG}:"
  tail -n 200 "${FEATURE_LOG}" || true
  exit ${FEATURE_RC}
fi

# verify features output
if [ ! -d "${FEATURES_OUT}" ] || ! find "${FEATURES_OUT}" -type f \( -iname "*.parquet" -o -iname "*.snappy.parquet" \) -print -quit | grep -q .; then
  log "ERROR: Features output not found or empty: ${FEATURES_OUT}"
  tail -n 200 "${FEATURE_LOG}" || true
  exit 2
fi
log "✅ Features ready: ${FEATURES_OUT}"

# ---- 6) Build features from gold (trajectory-level = silver) ----
if [ -f "${BUILD_APP}" ]; then
  log "=== STEP 6: Build features from trajectory data (silver) ==="

  if "${SPARK_SUBMIT}" --master local[*] \
      "${BUILD_APP}" \
      --gold "${SILVER_OUT}" \
      --out "${FEATURES_OUT}" \
      > "${BUILD_LOG}" 2>&1; then
    log "Build-features finished. See ${BUILD_LOG}"
  else
    BUILD_RC=$?
    log "ERROR: Build-features FAILED (exit ${BUILD_RC}). Tail ${BUILD_LOG}:"
    tail -n 200 "${BUILD_LOG}" || true
    exit ${BUILD_RC}
  fi

  if [ ! -d "${FEATURES_OUT}" ] || ! find "${FEATURES_OUT}" -type f -iname "*.parquet" -print -quit | grep -q .; then
    log "ERROR: Features output not found or empty: ${FEATURES_OUT}"
    tail -n 200 "${BUILD_LOG}" || true
    exit 2
  fi

  log "✅ Features (trajectory-based) ready: ${FEATURES_OUT}"
else
  log "Skipping build_features step (app not present): ${BUILD_APP}"
fi

# ---- 7) Train model (features -> model) ----
log "=== STEP 7: Train ML model ==="
# typical args: --features <path> --model-out <path>
if "${SPARK_SUBMIT}" --master local[*] "${TRAIN_APP}" --features "${FEATURES_OUT}" --model-out "${MODEL_OUT}" > "${TRAIN_LOG}" 2>&1; then
  log "Training finished successfully. See ${TRAIN_LOG}"
else
  TRAIN_RC=$?
  log "ERROR: Training failed (exit ${TRAIN_RC}). Tail ${TRAIN_LOG}:"
  tail -n 200 "${TRAIN_LOG}" || true
  exit ${TRAIN_RC}
fi

# basic check: model directory must contain files
if ! find "${MODEL_OUT}" -mindepth 1 -maxdepth 3 -print -quit | grep -q .; then
  log "WARNING: Model output directory looks empty: ${MODEL_OUT}"
else
  log "✅ Model written to: ${MODEL_OUT}"
fi

log "=== ALL STEPS FINISHED SUCCESSFULLY ==="
exit 0
