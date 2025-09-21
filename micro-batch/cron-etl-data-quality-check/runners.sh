#!/usr/bin/env bash
set -euo pipefail

# =========================
# General config
# =========================
REGION="ap-southeast-3"
PG_SSL="false"
SECRET_ID="aws-secret-manager-id"
ATHENA_DB="glue-catalog-databasename-monitoring"
ATHENA_TBL="data_quality_check"
S3_OUTPUT="s3://bucket-name-result-query-athena/data-quality/databasename/"
WORKGROUP="primary"
SCRIPT="path-folder/dql.py"
PYTHON="path-folder/venv/bin/python"

# Target date (UTC)
TARGET_DATE_UTC="${1:-$(date -u +%F)}"
TARGET_START_DATE_UTC="${1:-$(date -u -d '2 days ago' +%F)}"

RUN_HOUR_LOG_WIB="$(TZ="Asia/Jakarta" date +%H:%M:%S)"

# Delay & retries
SLEEP_BETWEEN=2
MAX_RETRIES=1

# Heuristics: treat zero-row writes as failure? (0=no, 1=yes)
FAIL_ON_EMPTY="${FAIL_ON_EMPTY:-0}"

# WIB hour partition (e.g., 3x/day schedule)
RUN_HOUR_WIB="$(TZ="Asia/Jakarta" date +%H)"
RUN_HOUR_LOG_WIB="$(TZ="Asia/Jakarta" date +%H:%M:%S)"

# =========================
# Resolve full DATABASE_SOURCE from Secrets Manager
# =========================
DATABASE_SOURCE="${DATABASE_SOURCE:-}"
if [[ -z "${DATABASE_SOURCE}" ]]; then
  if command -v aws >/dev/null 2>&1 && command -v jq >/dev/null 2>&1; then
    if SECRET_JSON="$(aws secretsmanager get-secret-value \
        --secret-id "$SECRET_ID" \
        --region "$REGION" \
        --query 'SecretString' \
        --output text 2>/dev/null)"; then
      DATABASE_SOURCE="$(jq -r '.database // .dbname // empty' <<<"$SECRET_JSON" || true)"
    fi
  fi
  [[ -z "${DATABASE_SOURCE}" ]] && DATABASE_SOURCE="$(basename "$SECRET_ID")"
fi

# =========================
# Checks list
# Format per line:
# id|src_schema|src_table|slv_db|slv_table|src_ts|slv_ts|[src_pred]|[slv_pred]
# =========================
CHECKS_FILE="${CHECKS_FILE:-checks.txt}"
declare -a CHECKS=()
if [[ -f "$CHECKS_FILE" ]]; then
  while IFS= read -r line; do
    [[ -z "${line// }" ]] && continue
    [[ "${line:0:1}" == "#" ]] && continue
    CHECKS+=("$line")
  done < "$CHECKS_FILE"
else
  CHECKS+=(
    "tablename|public|tablename|glue-catalog-databasename-target|tablename|created_at|created_at"
    )
fi

# =========================
# Local logs (single CSV + per-table error txt on demand)
# =========================
LOG_DIR="./logs"
mkdir -p "$LOG_DIR"
LOG_CSV="${LOG_DIR}/runner_log_${TARGET_DATE_UTC}_${RUN_HOUR_WIB}.csv"

# Header (one file only)
echo "DATABASE_SOURCE,TABLE_NAME,RUN_START_ISO,RUN_END_ISO,JOB_STATUS,TOTAL_DURATIONS" > "$LOG_CSV"

# =========================
# Helpers
# =========================
get_field() { local s="$1"; local n="$2"; awk -F'\\|' -v idx="$n" '{print $idx}' <<< "$s"; }
now_wib_iso() { TZ="Asia/Jakarta" date +'%Y-%m-%dT%H:%M:%S%:z'; }

# Glue DB precheck (returns 0=exists, 1=missing)
glue_db_exists() {
  local db="$1"
  aws glue get-database --name "$db" --region "$REGION" >/dev/null 2>&1
}

# Log patterns that indicate failure even if rc==0
looks_like_error() {
  local err_file="$1"
  # common Athena/Trino/our logs
  grep -qE '^\[ERROR\]|SCHEMA_NOT_FOUND|Schema .+ does not exist|NoSuchObjectException|AccessDenied' "$err_file"
}

# Optional: treat empty writes as failure
looks_like_empty_write() {
  local out_file="$1"
  grep -qE '\[WARN\] No rows to write\.|\[OK\] upserted 0 rows' "$out_file"
}

# =========================
# Counters
# =========================
TOTAL_OK=0
TOTAL_ERR=0
TABLE_DURS=()

# =========================
# Run one table
# =========================
run_one() {
  local check_str="$1"
  local attempt=0
  local backoff=5
  local t_start t_end start_epoch end_epoch dur status rc=0

  local slv_db table_name
  slv_db="$(get_field "$check_str" 4)"
  table_name="$(get_field "$check_str" 5)"

  t_start="$(now_wib_iso)"
  start_epoch=$(date +%s)

  # files to capture outputs
  local out_file="${LOG_DIR}/out_${table_name}_${TARGET_DATE_UTC}_${RUN_HOUR_WIB}.log"
  local err_file="${LOG_DIR}/err_${table_name}_${TARGET_DATE_UTC}_${RUN_HOUR_WIB}.txt"

  while :; do
    # ---- Precheck Glue database ----
    if ! aws glue get-database --name "$slv_db" --region "$REGION" >/dev/null 2>&1; then
      end_epoch=$(date +%s); dur=$((end_epoch - start_epoch)); t_end="$(now_wib_iso)"
      status="ERR"
      {
        echo "[ERROR] Precheck failed: Glue/Athena database '$slv_db' does not exist in region '$REGION'."
        echo "Check string: $check_str"
        echo "Time (WIB): start=$t_start end=$t_end"
      } > "$err_file"
      echo "[$(date -u +'%F %T')] FAILED precheck db='$slv_db' for check='$check_str' (${dur}s)" >&2
      echo "\"$DATABASE_SOURCE\",\"$table_name\",\"$t_start\",\"$t_end\",\"$status\",${dur}" >> "$LOG_CSV"
      TABLE_DURS+=("$dur"); TOTAL_ERR=$((TOTAL_ERR+1))
      local S3_ERR_PREFIX="s3://etl-operational/logs/data-quality-logs/logs-errors/dt=${TARGET_DATE_UTC}/hour=${RUN_HOUR_WIB}/source=${DATABASE_SOURCE}"
      aws s3 cp "$err_file" "${S3_ERR_PREFIX}/${table_name}_error.txt" --region "$REGION" || true
      rm -f "$out_file" "$err_file"
      return 1
    fi

    echo "[$(date -u +'%F %T')] START   check='$check_str' (WIB start: $t_start)"
    if "$PYTHON" "$SCRIPT" \
      --region "$REGION" \
      --pg-ssl "$PG_SSL" \
      --secret-id "$SECRET_ID" \
      --athena-database "$ATHENA_DB" \
      --athena-table "$ATHENA_TBL" \
      --s3-output-path "$S3_OUTPUT" \
      --create-destination-if-missing \
      --workgroup "$WORKGROUP" \
      --start-date-utc "$TARGET_START_DATE_UTC" \
      --end-date-utc-exclusive "$TARGET_DATE_UTC" \
      --check "$check_str" \
      > >(tee "$out_file") \
      2> >(tee "$err_file" >&2)
    then rc=0; else rc=$?; fi

    # Heuristic: rc==0 but stderr shows errors
    if [[ $rc -eq 0 ]] && grep -qE '^\[ERROR\]|SCHEMA_NOT_FOUND|Schema .+ does not exist|NoSuchObjectException|AccessDenied' "$err_file"; then
      rc=10
    fi
    # Optional: consider empty writes a failure
    if [[ $rc -eq 0 && "${FAIL_ON_EMPTY:-0}" == "1" ]] && grep -qE '\[WARN\] No rows to write\.|\[OK\] upserted 0 rows' "$out_file"; then
      rc=11
    fi

    if [[ $rc -eq 0 ]]; then
      end_epoch=$(date +%s); dur=$((end_epoch - start_epoch)); t_end="$(now_wib_iso)"
      status="OK"
      echo "[$(date -u +'%F %T')] SUCCESS check='$check_str' (${dur}s)"
      echo "\"$DATABASE_SOURCE\",\"$table_name\",\"$t_start\",\"$t_end\",\"$status\",${dur}" >> "$LOG_CSV"
      TABLE_DURS+=("$dur"); TOTAL_OK=$((TOTAL_OK+1))
      rm -f "$out_file" "$err_file"
      return 0
    fi

    if (( attempt >= MAX_RETRIES )); then
      end_epoch=$(date +%s); dur=$((end_epoch - start_epoch)); t_end="$(now_wib_iso)"
      status="ERR"
      echo "[$(date -u +'%F %T')] FAILED  check='$check_str' after $((attempt+1)) attempt(s) rc=$rc (${dur}s)" >&2
      echo "\"$DATABASE_SOURCE\",\"$table_name\",\"$t_start\",\"$t_end\",\"$status\",${dur}" >> "$LOG_CSV"
      TABLE_DURS+=("$dur"); TOTAL_ERR=$((TOTAL_ERR+1))
      local S3_ERR_PREFIX="s3://etl-operational/logs/data-quality-logs/logs-errors/dt=${TARGET_DATE_UTC}/hour=${RUN_HOUR_WIB}/source=${DATABASE_SOURCE}"
      aws s3 cp "$err_file" "${S3_ERR_PREFIX}/${table_name}_error.txt" --region "$REGION" || true
      rm -f "$out_file" "$err_file"
      return 1
    fi

    attempt=$((attempt+1))
    echo "[$(date -u +'%F %T')] RETRY   check='$check_str' in ${backoff}s (attempt ${attempt}/${MAX_RETRIES}, last rc=$rc)" >&2
    sleep "$backoff"
    backoff=$((backoff*2))
  done
}

# =========================
# Main
# =========================
TOTAL=${#CHECKS[@]}
echo "Found $TOTAL checks. Target date (UTC) = ${TARGET_DATE_UTC} | WIB hour partition = ${RUN_HOUR_WIB} | DATABASE_SOURCE='${DATABASE_SOURCE}'"

WALL_START=$(date +%s)

idx=0
for chk in "${CHECKS[@]}"; do
  idx=$((idx+1))
  echo "---------- [$idx/$TOTAL] ----------"
  run_one "$chk" || true
  sleep "$SLEEP_BETWEEN"
done

WALL_END=$(date +%s)
TOTAL_DUR=$((WALL_END - WALL_START))

GRAND_DUR=0
for d in "${TABLE_DURS[@]}"; do GRAND_DUR=$((GRAND_DUR + d)); done

echo "---------------------------------------------------------------------------------------------------------------------------------------"
echo "Tables OK : $TOTAL_OK"
echo "Tables ERR: $TOTAL_ERR"
echo "Wall time : ${TOTAL_DUR}s (start to finish)"
echo "Sum of per-table durations: ${GRAND_DUR}s"
echo "---------------------------------------------------------------------------------------------------------------------------------------"

# =========================
# Upload CSV log to S3 (Hive partitions with hour)
# =========================
S3_LOGS_PREFIX="s3://etl-operational/logs/data-quality-logs/dql/dt=${TARGET_DATE_UTC}/hour=${RUN_HOUR_WIB}/source=${DATABASE_SOURCE}"

aws s3 cp "$LOG_CSV" "${S3_LOGS_PREFIX}/runner_log_${RUN_HOUR_LOG_WIB}.csv" --region "$REGION"
echo "Main log uploaded to ${S3_LOGS_PREFIX}/runner_log_${RUN_HOUR_LOG_WIB}.csv"

# Remove local CSV after successful upload
rm -f "$LOG_CSV"
echo "Local log removed: $LOG_CSV"
