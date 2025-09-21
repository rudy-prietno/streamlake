#!/usr/bin/env bash
# ==============================================================================
# wr_runner_multi.sh — Multi-job, cron-safe runner for wr.py-style ETL
# Dedupe-aware (compatible with --dedupe-source & --dedupe-tiebreakers)
# ==============================================================================

set -Eeuo pipefail

##### === DEFAULT / GLOBAL CONFIG ===
PYTHON_BIN="path-folder/venv/bin/python"   # Python virtual environment
WR_PY="path-folder/micro_batch_data.py"    # ETL entrypoint script

AWS_REGION="ap-southeast-3"
AWS_PROFILE=""                             # leave empty to use instance role
S3_RUN_ROOT="s3://bucket-name/logs/cron-batch/databasename/"   # MUST end with '/'

# Temporary logs
LOG_DIR="/tmp/wr/databasename"
LOCK_DIR="/var/lock/wr/databasename"
KEEP_LOCAL_LOGS="${KEEP_LOCAL_LOGS:-0}"    # 0 = remove local after upload
mkdir -p "${LOG_DIR}" "${LOCK_DIR}"

# Default args to Python (can be overridden per job if supported by script)
SECRET_ID="aws-secret-manager-id"
MODE="full-run"
GLUE_DB="glue-databasename-silver"
ATHENA_OUTPUT="s3://bucket-name-result-query-athena/batch/cron/"
SSLMODE="disable"
WORKGROUP=""                               # optional

# === Dedupe defaults (can be overridden via job columns 12 & 13) ===
DEDUPE_SOURCE_DEFAULT="${DEDUPE_SOURCE_DEFAULT:-true}"
DEDUPE_TIEBREAKERS_DEFAULT="${DEDUPE_TIEBREAKERS_DEFAULT:-updated_at,id}"

# === Drop & Purge toggles (match Python flags) ===
DROP_AFTER_MERGE="${DROP_AFTER_MERGE:-1}"          # 1 => --drop-staging-after-merge
PURGE_STAGING_OBJECTS="${PURGE_STAGING_OBJECTS:-0}" # 1 => --purge-staging-objects

# === Debug toggle (global) ===
USE_DEBUG="${USE_DEBUG:-0}"
if [[ "$USE_DEBUG" == "1" ]]; then
  DEBUG_FLAG="--debug"
else
  DEBUG_FLAG=""
fi
ALIGN_DDL_DEFAULT="true"   # default --align-ddl if column 10 is empty

# Resource niceness wrappers
NICE_BIN="$(command -v nice || true)"
IONICE_BIN="$(command -v ionice || true)"
TIME_BIN="$(command -v /usr/bin/time || true)"
WRAP_PREFIX=""
[[ -n "${IONICE_BIN}" ]] && WRAP_PREFIX="${WRAP_PREFIX} ionice -c2 -n7"
[[ -n "${NICE_BIN}"   ]] && WRAP_PREFIX="${WRAP_PREFIX} nice -n 5"
[[ -n "${TIME_BIN}"   ]] && WRAP_PREFIX="${WRAP_PREFIX} /usr/bin/time -v"

# AWS env
export AWS_DEFAULT_REGION="${AWS_REGION}"
[[ -n "${AWS_PROFILE}" ]] && export AWS_PROFILE

# Prechecks (cron PATH is often minimal)
command -v aws     >/dev/null 2>&1 || { echo "ERROR: aws cli not found"; exit 1; }
command -v flock   >/dev/null 2>&1 || { echo "ERROR: flock not found"; exit 1; }
command -v openssl >/dev/null 2>&1 || echo "WARN: openssl not found (fallback to urandom)"
[[ -x "${PYTHON_BIN}" ]] || { echo "ERROR: PYTHON_BIN not executable: ${PYTHON_BIN}"; exit 1; }
[[ -f "${WR_PY}"    ]] || { echo "ERROR: script not found at ${WR_PY}"; exit 1; }

# Normalize S3 root
[[ "${S3_RUN_ROOT}" == */ ]] || S3_RUN_ROOT="${S3_RUN_ROOT}/"

# Global lock (prevent overlapping global runs)
GLOBAL_LOCK="${LOCK_DIR}/runner.lock"
exec 7>"${GLOBAL_LOCK}"
if ! flock -n 7; then
  echo "$(date '+%F %T%z') | SKIP: another global run is active (${GLOBAL_LOCK})"
  exit 0
fi

# =======================
# JOBS (pipe-delimited)
# Columns:
#   1: job_name
#   2: pg_schema
#   3: pg_table
#   4: pg_sql             (inline SQL OR "FILE:/absolute/path/to.sql")
#   5: s3_staging_path
#   6: staging_table
#   7: prod_table
#   8: pk_col
#   9: updated_at_col
#  10: align_ddl          (true|false|"" -> fallback ALIGN_DDL_DEFAULT)
#  11: debug (optional)   (true|false -> override global)
#  12: dedupe_source      (optional: true|false -> override global)
#  13: dedupe_tiebreakers (optional: csv, e.g., "event_time,ingest_ts")

JOBS=$(cat <<'EOF'
tablename|public|tablename|SELECT * FROM public.tablename WHERE updated_at >= (CURRENT_TIMESTAMP AT TIME ZONE 'UTC') - interval '2 hours 30 minutes' AND updated_at <  (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')|s3://bucket-name-stg/staging/databasename/tablename/|stg_tablename|tablename|id|updated_at|true
EOF
)
# =======================

# Append-only logger
log() { local f="$1"; shift; printf "%s | %s\n" "$(date '+%F %T%z')" "$*" >> "$f"; }

# 8-hex random string
rand8() { command -v openssl >/dev/null 2>&1 && openssl rand -hex 4 || head -c 4 /dev/urandom | od -An -tx1 | tr -d ' \n' | cut -c1-8; }

# Upload log + marker + status.json (and clean up local file)
upload_and_cleanup() {
  local status="$1" rc="$2" job_name="$3" log_file="$4" start_epoch="$5" end_epoch="$6" s3_prefix="$7" \
        pg_schema="$8" pg_table="$9" staging_table="${10}" prod_table="${11}"

  local duration=$(( end_epoch - start_epoch ))

  # Timestamps (UTC & WIB)
  local start_utc end_utc
  start_utc="$(date -u -d "@${start_epoch}" '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || date -u -r "${start_epoch}" '+%Y-%m-%dT%H:%M:%SZ')"
  end_utc="$(date -u -d "@${end_epoch}"   '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null   || date -u -r "${end_epoch}"   '+%Y-%m-%dT%H:%M:%SZ')"
  WIB_OFFSET_SEC=25200
  start_wib="$(date -u -d "@$(( start_epoch + WIB_OFFSET_SEC ))" '+%Y-%m-%dT%H:%M:%S+0700')"
  end_wib="$(date -u -d "@$(( end_epoch   + WIB_OFFSET_SEC ))" '+%Y-%m-%dT%H:%M:%S+0700')"

  aws s3 cp "${log_file}" "${s3_prefix}log.txt" --no-progress --only-show-errors || true

  if [[ "${status}" == "SUCCESS" ]]; then
    printf '' | aws s3 cp - "${s3_prefix}_SUCCESS" --expected-size 0 --no-progress --only-show-errors || true
  else
    printf '' | aws s3 cp - "${s3_prefix}_ERROR"   --expected-size 0 --no-progress --only-show-errors || true
  fi

  printf '%s\n' "{
    \"run_id\": \"${run_id}\",
    \"job\": \"${job_name}\",
    \"exit_code\": ${rc},
    \"status\": \"${status}\",
    \"host\": \"${host}\",
    \"pg_schema\": \"${pg_schema}\",
    \"pg_table\": \"${pg_table}\",
    \"staging_table\": \"${staging_table}\",
    \"prod_table\": \"${prod_table}\",
    \"started_at_utc\": \"${start_utc}\",
    \"ended_at_utc\": \"${end_utc}\",
    \"started_at_wib\": \"${start_wib}\",
    \"ended_at_wib\": \"${end_wib}\",
    \"timezone_wib\": \"Asia/Jakarta\",
    \"duration_seconds\": ${duration},
    \"log_s3_uri\": \"${s3_prefix}log.txt\"
  }" | aws s3 cp - "${s3_prefix}monitoring/status.json" --content-type "application/json" --no-progress --only-show-errors || true

  if [[ "${KEEP_LOCAL_LOGS}" != "1" ]]; then
    rm -f "${log_file}" || true
  fi
}

# --- Helpers for normalizing tiebreakers ---
trim() { local x="${1}"; x="${x#"${x%%[![:space:]]*}"}"; x="${x%"${x##*[![:space:]]}"}"; printf '%s' "$x"; }

resolve_tiebreakers() {
  # arg1: csv tiebreakers (can be empty)
  # arg2: pk_col (e.g., "id", "unique_id", "merchant_id", or multi-PK "a,b")
  # arg3: updated_at_col (e.g., "updated_at" or "created_at")
  local csv="$1"; local pk="$2"; local ucol="$3"
  local parts=() out=()
  declare -A seen

  IFS=',' read -r -a parts <<< "$csv"
  for raw in "${parts[@]}"; do
    local t; t="$(trim "$raw")"
    [[ -z "$t" ]] && continue
    [[ "$t" == "id" ]] && t="$pk"
    [[ "$t" == "updated_at" && -n "$ucol" ]] && t="$ucol"
    # Split if mapping results in multi-columns, e.g., "a,b"
    IFS=',' read -r -a maybe_multi <<< "$t"
    for tt in "${maybe_multi[@]}"; do
      tt="$(trim "$tt")"
      [[ -z "$tt" ]] && continue
      if [[ -z "${seen[$tt]+x}" ]]; then
        seen[$tt]=1
        out+=("$tt")
      fi
    done
  done
  (IFS=','; printf '%s' "${out[*]}")
}

# Run a single job
run_one_job() {
  local job_name="$1" pg_schema="$2" pg_table="$3" pg_sql_raw="$4" s3_staging="$5" \
        staging_table="$6" prod_table="$7" pk_col="$8" updated_at_col="$9" align_ddl="${10}" debug_flag="${11}" \
        dedupe_source="${12}" dedupe_tiebreakers="${13}"

  # Per-job lock
  local job_lock="${LOCK_DIR}/wr_${job_name}.lock"
  exec 9>"${job_lock}"
  if ! flock -n 9; then
    echo "$(date '+%F %T%z') | SKIP job=${job_name}: already running (${job_lock})"
    return 0
  fi

  # Run ID & log
  local run_ts_utc; run_ts_utc="$(date -u +%Y%m%dT%H%M%SZ)"
  run_id="${job_name}-${run_ts_utc}-$(rand8)"
  local log_file; log_file="$(mktemp "${LOG_DIR}/${run_id}.XXXX.log")"
  host="$(hostname)"

  # Resolve SQL (inline vs file)
  local pg_sql="${pg_sql_raw}"
  if [[ "${pg_sql_raw}" == FILE:* ]]; then
    local sql_path="${pg_sql_raw#FILE:}"
    if [[ -f "${sql_path}" ]]; then
      pg_sql="$(<"${sql_path}")"
    else
      local start_epoch; start_epoch="$(date +%s)"
      log "${log_file}" "ERROR job=${job_name}: SQL file not found: ${sql_path}"
      local end_epoch; end_epoch="$(date +%s)"
      local yyyy mm dd s3_prefix
      yyyy="$(date -u +%Y)"; mm="$(date -u +%m)"; dd="$(date -u +%d)"
      s3_prefix="${S3_RUN_ROOT}${job_name}/${yyyy}/${mm}/${dd}/${run_id}/"
      upload_and_cleanup "ERROR" 2 "${job_name}" "${log_file}" "${start_epoch}" "${end_epoch}" "${s3_prefix}" \
                         "${pg_schema}" "${pg_table}" "${staging_table}" "${prod_table}"
      return 2
    fi
  fi

  # --align-ddl
  local align_flag=""
  if [[ "${align_ddl:-}" == "true" ]] || { [[ -z "${align_ddl:-}" ]] && [[ "${ALIGN_DDL_DEFAULT}" == "true" ]]; }; then
    align_flag="--align-ddl"
  fi

  # Optional flags
  local drop_flag="";  [[ "${DROP_AFTER_MERGE}" == "1" ]] && drop_flag="--drop-staging-after-merge"
  local purge_flag=""; [[ "${PURGE_STAGING_OBJECTS}" == "1" ]] && purge_flag="--purge-staging-objects"

  # Arg array (safe from quoting issues)
  args=()
  args+=(--mode "${MODE}")
  args+=(--secret-id "${SECRET_ID}")
  args+=(--region "${AWS_REGION}")
  args+=(--pg-sql "${pg_sql}")
  args+=(--pg-schema "${pg_schema}")
  args+=(--pg-table "${pg_table}")
  [[ -n "${align_flag}" ]] && args+=("${align_flag}")
  args+=(--s3-staging-path "${s3_staging}")
  args+=(--glue-db "${GLUE_DB}")
  args+=(--staging-table "${staging_table}")
  args+=(--prod-table "${prod_table}")
  args+=(--op-literal "cron-2-hours")
  args+=(--pk "${pk_col}")
  args+=(--updated-at-col "${updated_at_col}")
  args+=(--athena-output "${ATHENA_OUTPUT}")
  [[ -n "${WORKGROUP}" ]] && args+=(--workgroup "${WORKGROUP}")
  args+=(--sslmode "${SSLMODE}")

  # Dedupe flags
  if [[ "${dedupe_source}" == "true" ]]; then
    args+=(--dedupe-source)
  fi
  if [[ -n "${dedupe_tiebreakers}" ]]; then
    args+=(--dedupe-tiebreakers "${dedupe_tiebreakers}")
  fi

  [[ -n "${drop_flag}"  ]] && args+=("${drop_flag}")
  [[ -n "${purge_flag}" ]] && args+=("${purge_flag}")
  [[ -n "${debug_flag}" ]] && args+=("${debug_flag}")

  # Exec
  local start_epoch; start_epoch="$(date +%s)"
  log "${log_file}" "START job=${job_name} run_id=${run_id}"

  set +e
  # shellcheck disable=SC2086
  ${WRAP_PREFIX} "${PYTHON_BIN}" "${WR_PY}" "${args[@]}" >> "${log_file}" 2>&1
  local rc=$?
  set -e

  # Finalize
  local end_epoch; end_epoch="$(date +%s)"
  local status="ERROR"; [[ ${rc} -eq 0 ]] && status="SUCCESS"

  local yyyy mm dd s3_prefix
  yyyy="$(date -u +%Y)"; mm="$(date -u +%m)"; dd="$(date -u +%d)"
  s3_prefix="${S3_RUN_ROOT}${job_name}/${yyyy}/${mm}/${dd}/${run_id}/"

  upload_and_cleanup "${status}" "${rc}" "${job_name}" "${log_file}" "${start_epoch}" "${end_epoch}" "${s3_prefix}" \
                     "${pg_schema}" "${pg_table}" "${staging_table}" "${prod_table}"

  return ${rc}
}

# === MAIN LOOP ===
while IFS= read -r line; do
  # Skip blank/comment lines
  [[ -z "${line// }" ]] && continue
  [[ "${line}" =~ ^# ]] && continue

  # Parse up to 13 columns (others optional)
  IFS='|' read -r job_name pg_schema pg_table pg_sql s3_staging staging_table prod_table pk_col updated_at_col \
                 align_ddl job_debug dedupe_source dedupe_tiebreakers <<< "${line}"

  # Validate required columns
  missing=false
  for must in job_name pg_schema pg_table pg_sql s3_staging staging_table prod_table pk_col updated_at_col; do
    if [[ -z "${!must:-}" ]]; then
      echo "SKIP: missing required column '${must}' in row: ${line}"
      missing=true
      break
    fi
  done
  $missing && continue

  # Debug per-job
  job_debug="${job_debug:-}"
  if [[ "$job_debug" == "true" ]]; then
    job_debug_flag="--debug"
  elif [[ "$job_debug" == "false" ]]; then
    job_debug_flag=""
  else
    job_debug_flag="${DEBUG_FLAG}"
  fi

  # Dedupe per-job (fallback to global defaults)
  dedupe_source="${dedupe_source:-$DEDUPE_SOURCE_DEFAULT}"
  dedupe_tiebreakers="${dedupe_tiebreakers:-$DEDUPE_TIEBREAKERS_DEFAULT}"

  # Normalize tiebreakers: "updated_at" -> updated_at_col, "id" -> pk_col
  dedupe_tiebreakers="$(resolve_tiebreakers "${dedupe_tiebreakers}" "${pk_col}" "${updated_at_col}")"

  # Execute job
  if ! run_one_job "${job_name}" "${pg_schema}" "${pg_table}" "${pg_sql}" "${s3_staging}" \
                   "${staging_table}" "${prod_table}" "${pk_col}" "${updated_at_col}" "${align_ddl:-}" \
                   "${job_debug_flag}" "${dedupe_source}" "${dedupe_tiebreakers}"; then
    echo "$(date '+%F %T%z') | WARN: job ${job_name} finished with errors (see S3 monitoring/status.json)"
  fi
done <<< "${JOBS}"
