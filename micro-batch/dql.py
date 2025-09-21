#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
dql.py
============

A utility to compare per-hour row counts between PostgreSQL (source)
and Athena Iceberg (silver), then write the comparison results back
into an Iceberg table using MERGE INTO.

Usage Examples
--------------

# Single-day window (UTC): 2025-08-31
python dq_hourly.py \
  --secret-id <aws-secret-manager-id> \
  --athena-database glue-databasename-monitoring \
  --athena-table data_quality_check \
  --s3-output-path s3://bucket-name-athena-results/monitoring/ \
  --workgroup primary \
  --start-date-utc 2025-08-31 \
  --pg-ssl false \
  --check "tablename|public|tablename|glue-database-name-target|tablename|created_at|created_at"

# Multi-day window: 2025-08-30 through 2025-08-31 (exclusive 2025-09-01)
python dq_hourly.py \
  --secret-id <aws-secret-manager-id> \
  --athena-database glue-databasename-monitoring \
  --athena-table data_quality_check \
  --s3-output-path s3://bucket-name-athena-results/monitoring/ \
  --workgroup primary \
  --start-date-utc 2025-08-30 \
  --end-date-utc-exclusive 2025-09-01 \
  --pg-ssl false \
  --check "tablename|public|tablename|glue-database-name-target|tablename|created_at|created_at"

Key Arguments
-------------
--secret-id                : AWS Secrets Manager secret for PostgreSQL credentials
--athena-database          : Glue/Athena database containing the Iceberg table
--athena-table             : Target Iceberg table name (without DB prefix)
--s3-output-path           : S3 bucket/prefix used for Athena query staging
--workgroup                : Athena workgroup (must support DML/MERGE)
--start-date-utc           : Start date (YYYY-MM-DD, inclusive)
--end-date-utc-exclusive   : End date (YYYY-MM-DD, exclusive). Optional, defaults to +1 day if omitted
--target-date-utc          : Legacy single-day option if start/end not provided
--pg-ssl                   : Use SSL for PostgreSQL connection ("true" / "false")
--check                    : Format "id|src_schema|src_table|slv_db|slv_table|src_ts|slv_ts|src_pred|slv_pred"

Output
------
- Per-hour (UTC) DataFrame with columns:
  created_at (date), created_hours (HH:00:00),
  total_row_source, total_row_silver, diff, status, large, ingestion_at
- Results are merged into the Iceberg table via MERGE (key: id + created_at + created_hours)
"""

import argparse
import json
import os
import sys
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass

import boto3
import awswrangler as wr
import pg8000.native
import pandas as pd
import ssl
from datetime import datetime, date, timedelta, timezone
import time
import random
import hashlib

# ----------------------------
# TZ helpers
# ----------------------------
WIB_OFFSET_HOURS = 7

def today_utc_date() -> date:
    return datetime.now(timezone.utc).date()

def now_wib_naive_ts() -> datetime:
    """WIB timestamp (naive) for ingestion_at."""
    return (datetime.now(timezone.utc) + timedelta(hours=WIB_OFFSET_HOURS)).replace(tzinfo=None)

# ----------------------------
# Grid helpers
# ----------------------------
def ensure_hour_grid_range(
    df: pd.DataFrame,
    value_col: str,
    start_dt: datetime,
    end_dt: datetime,
    hours_col: str = "created_hours",
) -> pd.DataFrame:
    """Build a full per-hour grid for [start_dt, end_dt) and left-join df (which must have hours_col & value_col).
       NOTE: this helper uses full 'YYYY-MM-DD HH:00:00' slots internally for safe multi-day alignment."""
    slots = []
    t = start_dt
    while t < end_dt:
        slots.append(t.strftime("%Y-%m-%d %H:00:00"))
        t += timedelta(hours=1)
    grid = pd.DataFrame({hours_col: slots})
    if df.empty:
        out = grid.copy()
        out[value_col] = 0
        out[value_col] = out[value_col].astype("int64")
        return out
    need = {hours_col, value_col}
    if not need.issubset(set(df.columns)):
        raise ValueError(f"ensure_hour_grid_range: missing columns {need - set(df.columns)}")
    out = grid.merge(df[[hours_col, value_col]], on=hours_col, how="left").fillna({value_col: 0})
    out[value_col] = out[value_col].astype("int64")
    return out

def backoff_sleep(attempt: int) -> None:
    base = min(60, 2 ** attempt)
    time.sleep(base + random.random() * 0.5)

def str2bool(v: str) -> bool:
    return str(v).lower() in ("1", "true", "t", "yes", "y")

def _md5_hex(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()

# ----------------------------
# Secrets Manager
# ----------------------------
def load_pg_conn_params_from_secret(session: boto3.Session, secret_id: str) -> Dict[str, Any]:
    sm = session.client("secretsmanager")
    sec = sm.get_secret_value(SecretId=secret_id)
    data = json.loads(sec.get("SecretString") or "{}")

    host = data.get("host")
    port = int(data.get("port", 5432))
    user = data.get("user") or data.get("username")
    password = data.get("password")
    database = data.get("database") or data.get("dbname")

    missing = [k for k, v in [("host", host), ("port", port), ("user", user),
                              ("password", password), ("database", database)] if not v]
    if missing:
        raise ValueError(f"Secret {secret_id} missing: {', '.join(missing)}")
    return {"host": host, "port": port, "user": user, "password": password, "database": database}

# ----------------------------
# Clients
# ----------------------------
class PostgresClient:
    def __init__(self, host: str, port: int, database: str, user: str, password: str, use_ssl: bool) -> None:
        self.host = host
        self.port = int(port)
        self.database = database
        self.user = user
        self.password = password
        self.use_ssl = bool(use_ssl)
        self._conn: Optional[pg8000.native.Connection] = None

    def connect(self) -> None:
        ctx = ssl.create_default_context() if self.use_ssl else None
        self._conn = pg8000.native.Connection(
            user=self.user, password=self.password, host=self.host, port=self.port, database=self.database, ssl_context=ctx
        )

    def close(self) -> None:
        try:
            if self._conn:
                self._conn.close()
        finally:
            self._conn = None

    def fetch_hourly_counts_range_utc(
        self,
        schema: str,
        table: str,
        start_utc: datetime,
        end_utc: datetime,
        timestamp_column: str,
        predicates: str = ""
    ) -> pd.DataFrame:
        """Per-hour (UTC) counts for [start_utc, end_utc)."""
        where_extra = f" AND ({predicates})" if predicates.strip() else ""
        sql = f"""
            SELECT
              date_trunc('hour', {timestamp_column}) AS h_utc,
              COUNT(1)::bigint AS ct
            FROM {schema}.{table}
            WHERE {timestamp_column} >= :start::timestamp
              AND {timestamp_column} <  :end::timestamp
              {where_extra}
            GROUP BY 1
        """

        rows = self._conn.run(sql, start=start_utc, end=end_utc)
        df = pd.DataFrame(rows, columns=["h_utc", "ct"])

        if df.empty:
            return pd.DataFrame(columns=["created_hours", "total_row_source"]).pipe(
                ensure_hour_grid_range, value_col="total_row_source", start_dt=start_utc, end_dt=end_utc
            )

        # Full datetime-hour label first (safe merge), we will convert to HH:00:00 later in run_all()
        df["created_hours"] = pd.to_datetime(df["h_utc"]).dt.strftime("%Y-%m-%d %H:00:00")
        df = df[["created_hours", "ct"]].rename(columns={"ct": "total_row_source"})
        return ensure_hour_grid_range(df, "total_row_source", start_dt=start_utc, end_dt=end_utc)

class AthenaClient:
    def __init__(self, boto3_session: Optional[boto3.Session] = None, workgroup: Optional[str] = None) -> None:
        self.session = boto3_session or boto3.Session()
        self.workgroup = workgroup
        self._glue = self.session.client("glue")

    def assert_database_exists(self, database: str) -> None:
        """Raise ValueError if Glue/Athena database does not exist."""
        try:
            self._glue.get_database(Name=database)
        except self._glue.exceptions.EntityNotFoundException:
            raise ValueError(f"Athena/Glue database not found: '{database}'")
        except Exception as e:
            raise RuntimeError(f"Glue get_database error for '{database}': {e}")

    def read_sql(self, sql: str, database: str, max_attempts: int = 4) -> pd.DataFrame:
        last_err = None
        for attempt in range(1, max_attempts + 1):
            try:
                return wr.athena.read_sql_query(
                    sql=sql,
                    database=database,
                    boto3_session=self.session,
                    workgroup=self.workgroup
                )
            except Exception as e:
                last_err = e
                if attempt == max_attempts:
                    raise
                backoff_sleep(attempt)
        raise last_err  # type: ignore[misc]

    def hourly_counts_range_utc(
        self,
        database: str,
        table: str,
        start_utc: datetime,
        end_utc: datetime,
        timestamp_column: str,
        predicates: str = ""
    ) -> pd.DataFrame:
        """Per-hour (UTC) counts for [start_utc, end_utc)."""
        self.assert_database_exists(database)
        where_extra = f" AND ({predicates})" if predicates.strip() else ""
        start = start_utc.strftime("%Y-%m-%d %H:%M:%S")
        end   = end_utc.strftime("%Y-%m-%d %H:%M:%S")

        sql = f"""
        WITH base AS (
          SELECT
            date_trunc('hour', {timestamp_column}) AS h_utc
          FROM "{database}"."{table}"
          WHERE {timestamp_column} >= timestamp '{start}'
            AND {timestamp_column}  < timestamp '{end}'
            {where_extra}
        )
        SELECT
          date_format(h_utc, '%Y-%m-%d %H:00:00') AS created_hours,
          CAST(COUNT(1) AS BIGINT) AS total_row_silver
        FROM base
        GROUP BY 1
        ORDER BY 1
        """
        df = self.read_sql(sql, database=database)
        if df.empty:
            return pd.DataFrame(columns=["created_hours", "total_row_silver"]).pipe(
                ensure_hour_grid_range, value_col="total_row_silver", start_dt=start_utc, end_dt=end_utc
            )
        # produced with full datetime-hour label, will be converted to HH:00:00 later
        return ensure_hour_grid_range(df, "total_row_silver", start_dt=start_utc, end_dt=end_utc)

# ----------------------------
# Runner
# ----------------------------
@dataclass
class CheckSpec:
    id: str
    src_schema: str
    src_table: str
    slv_db: str
    slv_table: str
    src_ts: str
    slv_ts: str
    src_pred: str = ""
    slv_pred: str = ""

def parse_check_arg(s: str) -> CheckSpec:
    parts = s.split("|")
    if len(parts) < 7:
        raise ValueError(f"--check malformed (need >=7 fields): {s}")
    while len(parts) < 9:
        parts.append("")
    return CheckSpec(
        id=parts[0].strip(),
        src_schema=parts[1].strip(),
        src_table=parts[2].strip(),
        slv_db=parts[3].strip(),
        slv_table=parts[4].strip(),
        src_ts=parts[5].strip(),
        slv_ts=parts[6].strip(),
        src_pred=parts[7].strip(),
        slv_pred=parts[8].strip(),
    )

def _sql_str(s: str) -> str:
    """Escape single quotes for SQL string literals."""
    return s.replace("'", "''")

def _athena_wait(client, qid: str, sleep_sec: float = 1.2) -> None:
    """Poll Athena until SUCCEEDED/FAILED/CANCELLED."""
    while True:
        resp = client.get_query_execution(QueryExecutionId=qid)
        state = resp["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            if state != "SUCCEEDED":
                reason = resp["QueryExecution"]["Status"].get("StateChangeReason", "")
                raise RuntimeError(f"Athena DML {state}: {reason}")
            return
        time.sleep(sleep_sec)

class DQHourlyRunner:
    def __init__(
        self,
        secret_id: str,
        athena_database: str,   # destination Glue DB for Iceberg table
        athena_table: str,      # destination Iceberg table name
        s3_output_path: str,    # used as Athena query result staging path
        create_destination_if_missing: bool,  # ignored for Iceberg (kept for CLI compat)
        workgroup: Optional[str] = None,
        region: str = "ap-southeast-3",
        pg_use_ssl: bool = False,
    ) -> None:
        self.session = boto3.Session(region_name=region)
        self.athena_db = athena_database
        self.athena_tbl = athena_table
        self.s3_out = s3_output_path.rstrip("/")
        self.workgroup = workgroup

        # Athena client for DML
        self.athena_client = self.session.client("athena")

        # Load PG creds
        creds = load_pg_conn_params_from_secret(self.session, secret_id)
        self.pg_dbname = creds["database"]          # for database_source column
        self.pg = PostgresClient(
            host=creds["host"],
            port=creds["port"],
            database=creds["database"],
            user=creds["user"],
            password=creds["password"],
            use_ssl=pg_use_ssl,
        )

        # Athena reader
        self.athena = AthenaClient(self.session, workgroup=workgroup)

        # Track errors across checks
        self.had_error: bool = False

    def run_all(self, specs: List[CheckSpec], start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
        frames: List[pd.DataFrame] = []
        self.pg.connect()
        try:
            for spec in specs:
                t0 = time.perf_counter()
                try:
                    # Precheck target Glue DB exists
                    self.athena.assert_database_exists(spec.slv_db)

                    df_src = self.pg.fetch_hourly_counts_range_utc(
                        schema=spec.src_schema,
                        table=spec.src_table,
                        start_utc=start_dt,
                        end_utc=end_dt,
                        timestamp_column=spec.src_ts,
                        predicates=spec.src_pred,
                    )
                    df_slv = self.athena.hourly_counts_range_utc(
                        database=spec.slv_db,
                        table=spec.slv_table,
                        start_utc=start_dt,
                        end_utc=end_dt,
                        timestamp_column=spec.slv_ts,
                        predicates=spec.slv_pred,
                    )
                except Exception as e:
                    print(f"[ERROR] check={spec.id}: {e}", file=sys.stderr)
                    self.had_error = True
                    continue

                # Merge on full datetime-hour label; then convert to hour-only afterwards
                df = df_slv.merge(df_src, on="created_hours", how="outer").fillna(0)
                df["total_row_silver"] = df["total_row_silver"].astype("int64")
                df["total_row_source"] = df["total_row_source"].astype("int64")
                df["diff"] = (df["total_row_silver"] - df["total_row_source"]).abs().astype("int64")
                df["status"] = df["diff"].apply(lambda x: "SAME" if x == 0 else "DIFF")

                def _large(r) -> str:
                    if r["total_row_silver"] > r["total_row_source"]:
                        return "silver"
                    if r["total_row_silver"] < r["total_row_source"]:
                        return "source"
                    return "same"

                df["large"] = df.apply(_large, axis=1)

                # ---- derive created_at and overwrite created_hours to hour-only (HH:00:00) ----
                _slot = pd.to_datetime(df["created_hours"])           # still 'YYYY-MM-DD HH:00:00'
                df["created_at"] = _slot.dt.date                      # date
                df["created_hours"] = _slot.dt.strftime("%H:00:00")   # hour-only per requirement
                df["ingestion_at"] = now_wib_naive_ts()

                # Identifiers
                df["database_source"] = self.pg_dbname
                df["catalog_name"] = spec.slv_db
                df["table_name"] = spec.slv_table

                # id = md5(database_source|catalog_name|table_name|created_at)
                df["id"] = df.apply(
                    lambda r: _md5_hex(f"{r['database_source']}|{r['catalog_name']}|{r['table_name']}|{pd.to_datetime(r['created_at']).date()}"),
                    axis=1
                )

                df = df[[
                    "id",
                    "database_source",
                    "catalog_name",
                    "table_name",
                    "created_at",
                    "created_hours",
                    "total_row_silver",
                    "total_row_source",
                    "status",
                    "diff",
                    "large",
                    "ingestion_at"
                ]]
                frames.append(df)

                dur = time.perf_counter() - t0
                print(f"[TIME] check={spec.id} took {dur:.2f}s")
        finally:
            self.pg.close()

        if not frames:
            cols = ["id","database_source","catalog_name","table_name","created_at","created_hours",
                    "total_row_silver","total_row_source","status","diff","large","ingestion_at"]
            return pd.DataFrame(columns=cols)
        return pd.concat(frames, ignore_index=True)

    # ----------------------------
    # Iceberg MERGE writer (key: id + created_hours)
    # ----------------------------
    def write_results(self, df: pd.DataFrame, batch_size: int = 500) -> None:
        if df.empty:
            print("[WARN] No rows to write.")
            return

        df = df.copy()
        df["created_at"] = pd.to_datetime(df["created_at"]).dt.date
        df["created_hours"] = df["created_hours"].astype(str)     # HH:00:00
        df["total_row_silver"] = df["total_row_silver"].astype("int64")
        df["total_row_source"] = df["total_row_source"].astype("int64")
        df["status"] = df["status"].astype(str)
        df["diff"] = df["diff"].astype("int64")
        df["large"] = df["large"].astype(str)
        df["ingestion_at"] = pd.to_datetime(df["ingestion_at"])
        df["id"] = df["id"].astype(str)

        rows = df.to_dict("records")

        def esc(s: str) -> str:
            return s.replace("'", "''")

        def row_to_tuple(r: Dict[str, Any]) -> str:
            ingestion_ts = pd.to_datetime(r["ingestion_at"]).strftime("%Y-%m-%d %H:%M:%S")
            return (
                "("
                f"'{esc(r['id'])}',"
                f"'{esc(r['database_source'])}',"
                f"'{esc(r['catalog_name'])}',"
                f"'{esc(r['table_name'])}',"
                f"DATE '{r['created_at']}',"
                f"'{esc(r['created_hours'])}',"
                f"{int(r['total_row_silver'])},"
                f"{int(r['total_row_source'])},"
                f"'{esc(r['status'])}',"
                f"{int(r['diff'])},"
                f"'{esc(r['large'])}',"
                f"TIMESTAMP '{ingestion_ts}'"
                ")"
            )

        for i in range(0, len(rows), batch_size):
            chunk = rows[i:i + batch_size]
            values_sql = ",\n".join(row_to_tuple(r) for r in chunk)

            sql = f"""
            MERGE INTO "{self.athena_db}"."{self.athena_tbl}" AS t
            USING (
                SELECT * FROM (
                    VALUES
                    {values_sql}
                ) AS v(
                    id,
                    database_source, catalog_name, table_name,
                    created_at, created_hours,
                    total_row_silver, total_row_source,
                    status, diff, large, ingestion_at
                )
            ) AS src
            ON  t.id = src.id
            AND t.created_at = src.created_at
            AND t.created_hours = src.created_hours
            WHEN MATCHED THEN UPDATE SET
                database_source   = src.database_source,
                catalog_name      = src.catalog_name,
                table_name        = src.table_name,
                created_at        = src.created_at,
                created_hours     = src.created_hours,
                total_row_silver  = src.total_row_silver,
                total_row_source  = src.total_row_source,
                status            = src.status,
                diff              = src.diff,
                large             = src.large,
                ingestion_at      = src.ingestion_at
            WHEN NOT MATCHED THEN INSERT (
                id, database_source, catalog_name, table_name,
                created_at, created_hours,
                total_row_silver, total_row_source,
                status, diff, large, ingestion_at
            ) VALUES (
                src.id, src.database_source, src.catalog_name, src.table_name,
                src.created_at, src.created_hours,
                src.total_row_silver, src.total_row_source,
                src.status, src.diff, src.large, src.ingestion_at
            )
            """

            resp = self.athena_client.start_query_execution(
                QueryString=sql,
                QueryExecutionContext={"Database": self.athena_db},
                WorkGroup=self.workgroup if self.workgroup else "primary",
                ResultConfiguration={"OutputLocation": f"{self.s3_out}/athena_tmp/"},
            )
            _athena_wait(self.athena_client, resp["QueryExecutionId"])

# ----------------------------
# CLI
# ----------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="DQ hourly compare (UTC) -> Iceberg MERGE via Athena.")
    p.add_argument("--secret-id", required=True, help="Secrets Manager SecretId (name or ARN) for PostgreSQL credentials.")
    p.add_argument("--athena-database", required=True, help="Glue/Athena database (destination Iceberg table's DB).")
    p.add_argument("--athena-table", required=True, help="Destination Iceberg table name (e.g., data_quality_check WITHOUT DB).")
    p.add_argument("--s3-output-path", required=True, help="S3 path used as Athena query result location (e.g., s3://bucket/prefix/).")
    p.add_argument("--create-destination-if-missing", action="store_true",
                   help="Ignored for Iceberg; kept for backward CLI compatibility.")
    p.add_argument("--workgroup", default=None, help="Athena workgroup (must allow DML on Iceberg).")
    p.add_argument("--target-date-utc", help="Target UTC date (YYYY-MM-DD). Used only if start/end not provided.")
    p.add_argument(
        "--start-date-utc",
        help="Start (UTC) date YYYY-MM-DD inclusive. If set without end, defaults to +1 day."
    )
    p.add_argument(
        "--end-date-utc-exclusive",
        help="End (UTC) date YYYY-MM-DD exclusive. Must be > start if provided."
    )
    p.add_argument(
        "--region",
        default=os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "ap-southeast-3")),
        help="AWS region for Secrets Manager/Athena (default: ap-southeast-3; env AWS_REGION/AWS_DEFAULT_REGION respected).",
    )
    p.add_argument(
        "--pg-ssl",
        default="false",
        choices=["true", "false"],
        help="Use SSL for PostgreSQL connection (default: false).",
    )
    p.add_argument("--check", action="append", required=True,
                   help='Repeatable. Format: "id|src_schema|src_table|slv_db|slv_table|src_ts|slv_ts|src_pred|slv_pred"')
    p.add_argument(
        "--fail-on-empty",
        default="false",
        choices=["true", "false"],
        help="If true, exit non-zero when no rows are written (default: false).",
    )
    return p.parse_args()

def resolve_window(args: argparse.Namespace) -> Tuple[datetime, datetime]:
    """Resolve [start, end) window based on CLI args."""
    if args.start_date_utc:
        start_dt = datetime.strptime(args.start_date_utc, "%Y-%m-%d")
        if args.end_date_utc_exclusive:
            end_dt = datetime.strptime(args.end_date_utc_exclusive, "%Y-%m-%d")
            if not (end_dt > start_dt):
                print("[FATAL] --end-date-utc-exclusive must be greater than --start-date-utc", file=sys.stderr)
                sys.exit(4)
        else:
            # default 1 day if end not provided
            end_dt = start_dt + timedelta(days=1)
        return start_dt, end_dt

    # fallback to target-date-utc (1 day)
    target_date = datetime.strptime(args.target_date_utc, "%Y-%m-%d").date() if args.target_date_utc else today_utc_date()
    start_dt = datetime(target_date.year, target_date.month, target_date.day)
    end_dt = start_dt + timedelta(days=1)
    return start_dt, end_dt

def main() -> None:
    t0_total = time.perf_counter()

    args = parse_args()
    start_dt, end_dt = resolve_window(args)
    specs = [parse_check_arg(s) for s in args.check]

    runner = DQHourlyRunner(
        secret_id=args.secret_id,
        athena_database=args.athena_database,
        athena_table=args.athena_table,
        s3_output_path=args.s3_output_path,  # used for Athena result staging
        create_destination_if_missing=args.create_destination_if_missing,  # ignored for Iceberg
        workgroup=args.workgroup,
        region=args.region,
        pg_use_ssl=str2bool(args.pg_ssl),
    )

    df = runner.run_all(specs, start_dt, end_dt)
    runner.write_results(df)
    dur_total = time.perf_counter() - t0_total

    # Summary
    window_str = f"{start_dt.strftime('%Y-%m-%d %H:%M:%S')} → {end_dt.strftime('%Y-%m-%d %H:%M:%S')} (exclusive)"
    print(f"[OK] upserted {len(df)} rows to {args.athena_database}.{args.athena_table} for window UTC: {window_str}")
    print(f"[TIME] total execution took {dur_total:.2f}s")

    # Propagate failure states to shell (so runner.sh can capture them)
    if runner.had_error:
        print("[FATAL] One or more checks failed.", file=sys.stderr)
        sys.exit(2)

    if str2bool(args.fail_on_empty) and df.empty and len(specs) > 0:
        print("[FATAL] No rows written for requested checks.", file=sys.stderr)
        sys.exit(3)

if __name__ == "__main__":
    main()
