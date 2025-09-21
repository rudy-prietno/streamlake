#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PostgreSQL → S3/Glue (Athena) staging + Iceberg MERGE
====================================================

This tool extracts data from PostgreSQL, stages it in S3 as Parquet with a Glue
Catalog table, and optionally merges the staging table into a production Iceberg
table in Athena. It can also drop the staging table and purge S3 objects after
a successful merge.

What this tool does:
  1. Extracts data from PostgreSQL using ``pg8000.native``.
  2. Writes a Parquet dataset to S3 and registers/updates a Glue Catalog table
     using ``awswrangler.s3.to_parquet``.
  3. Optionally executes an Athena MERGE to upsert the staging table into a
     production Iceberg table, with full type casting to match production schema.
  4. Optionally drops the staging table and purges S3 staging objects.

Key Features:
  * Credentials from AWS Secrets Manager or manual host/user/password.
  * Single explicit boto3 Session (region/profile) shared across Wrangler, Glue,
    and Athena operations.
  * TLS control via ``--sslmode`` and ``--ssl-no-verify`` for self-signed cases.
  * Robust column alignment:
      - Aligns to PostgreSQL DDL order when counts match.
      - Falls back to production Athena column order when counts differ
        (excluding ``__op`` and ``__ts_ms``).
      - Supports computed columns like ``unique_id``.
  * Safe staging schema: all staging columns default to ``string`` to avoid
    inference issues in Glue.
  * Type-aware MERGE:
      - ON clause casts staging PKs to destination types (prevents bigint=varchar).
      - SET/INSERT only includes production columns; staging-only columns ignored.
      - Supports ``__op`` (string literal from ``--op-literal``) and
        ``__ts_ms`` (Asia/Jakarta timestamp).
      - Optional ``updated_at`` guard column for safe idempotent merges.
  * Cleanup workflow: drop staging table and optionally purge S3 objects.
  * Helpful debugging with ``--debug`` flag.

Usage Examples:
  Stage only (PostgreSQL → S3/Glue):

    python micro_batch_data.py --mode stage-only \
      --secret-id glue/backfill/prod \
      --region ap-southeast-3 \
      --pg-sql "SELECT * FROM public.accounts" \
      --pg-schema public --pg-table accounts --align-ddl \
      --s3-staging-path s3://my-bucket/staging/accounts/ \
      --glue-db prod_silver --staging-table stg_accounts \
      --athena-output s3://my-bucket/athena/results/ \
      --sslmode disable --debug

  Merge only (Staging → Production Iceberg):

    python micro_batch_data.py --mode merge-only \
      --secret-id glue/backfill/prod \
      --region ap-southeast-3 \
      --region ap-southeast-3 \
      --glue-db prod_silver \
      --staging-table stg_order_product \
      --prod-table order_product_iceberg \
      --pk order_id,product_id \
      --updated-at-col updated_at \
      --op-literal cron-2-hours \
      --athena-output s3://my-bucket/athena/results/ \
      --workgroup primary \
      --dedupe-source\
      --dedupe-tiebreakers "created_at,order_id,product_id" \
      --debug

  Full run (Stage + Merge) with cleanup:

    python micro_batch_data.py --mode full-run \
      --secret-id glue/backfill/prod \
      --region ap-southeast-3 \
      --pg-sql "SELECT concat(order_id, '', product_id) AS unique_id, * FROM public.order_product" \
      --pg-schema public --pg-table order_product \
      --align-ddl --relax-align \
      --s3-staging-path s3://my-bucket/staging/order_product/ \
      --glue-db prod_silver --staging-table stg_order_product \
      --prod-table order_product_iceberg \
      --pk order_id,product_id \
      --updated-at-col updated_at \
      --op-literal cron-2-hours \
      --athena-output s3://my-bucket/athena/results/ \
      --workgroup primary \
      --drop-staging-after-merge \
      --purge-staging-objects \
      --sslmode require \
      --dedupe-source\
      --dedupe-tiebreakers "created_at,order_id,product_id" \
      --debug
"""


import argparse
import json
import os
import sys
from typing import Any, Dict, Iterable, List, Optional

import boto3
import awswrangler as wr
import pg8000.native
import pandas as pd
import ssl
from datetime import datetime, timezone
import re

# =============================================================================
# AWS Session & Secrets
# =============================================================================
def _get_boto3_session(region: Optional[str], profile: Optional[str]) -> boto3.session.Session:
    """Create a single explicit boto3 Session honoring region/profile."""
    kwargs: Dict[str, Any] = {}
    if profile:
        kwargs["profile_name"] = profile
    if region:
        kwargs["region_name"] = region
    return boto3.session.Session(**kwargs)


def fetch_secret_dict(secret_id: str, region: Optional[str] = None, profile: Optional[str] = None) -> Dict[str, Any]:
    """Fetch a JSON secret from Secrets Manager and return it as dict."""
    session = _get_boto3_session(region, profile)
    sm = session.client("secretsmanager")
    resp = sm.get_secret_value(SecretId=secret_id)
    secret_str = resp.get("SecretString")
    if not secret_str:
        raise RuntimeError(f"Secret {secret_id!r} has no SecretString payload.")
    return json.loads(secret_str)


# =============================================================================
# PostgreSQL: pg8000.native
# =============================================================================
def _ssl_context_from_sslmode(sslmode: Optional[str], no_verify: bool) -> Optional[ssl.SSLContext]:
    """
    Map sslmode to SSLContext:
      - None/'disable'/'none' -> None (no TLS)
      - else -> TLS on (verified by default; --ssl-no-verify to skip)
    """
    if not sslmode or str(sslmode).lower() in ("disable", "none"):
        return None
    if no_verify:
        return ssl._create_unverified_context()
    return ssl.create_default_context()


def _extract_conn_params_from_secret(secret: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize common secret keys into pg connection args."""
    s = {str(k).lower(): v for k, v in secret.items()}

    def pick(*names: str, default=None):
        for n in names:
            if n in s and s[n] not in (None, ""):
                return s[n]
        return default

    host = pick("host", "hostname")
    port = int(pick("port", default=5432))
    user = pick("username", "user")
    password = pick("password", "pwd")
    database = pick("dbname", "database", "db")
    sslmode = pick("sslmode", default=None)

    missing = [n for n, v in [("host", host), ("username", user), ("password", password), ("database", database)] if not v]
    if missing:
        raise RuntimeError(f"Secret missing required keys: {', '.join(missing)}")

    return {"host": host, "port": port, "user": user, "password": password, "database": database, "sslmode": sslmode}


def connect_postgres_pg8000(
    secret_id: Optional[str] = None,
    region: Optional[str] = None,
    profile: Optional[str] = None,
    host: Optional[str] = None,
    port: int = 5432,
    user: Optional[str] = None,
    password: Optional[str] = None,
    database: Optional[str] = None,
    sslmode: Optional[str] = None,
    ssl_no_verify: bool = False,
) -> pg8000.native.Connection:
    """Open a pg8000.native.Connection from Secrets Manager or explicit params."""
    if secret_id:
        secret = fetch_secret_dict(secret_id, region=region, profile=profile)
        params = _extract_conn_params_from_secret(secret)
    else:
        required = {"host": host, "user": user, "password": password, "database": database}
        missing = [k for k, v in required.items() if not v]
        if missing:
            raise RuntimeError(f"Missing connection params: {', '.join(missing)}")
        params = {"host": host, "port": int(port), "user": user, "password": password, "database": database, "sslmode": None}

    if sslmode is not None:
        params["sslmode"] = sslmode

    ssl_context = _ssl_context_from_sslmode(params.get("sslmode"), ssl_no_verify)

    return pg8000.native.Connection(
        user=params["user"],
        host=params["host"],
        port=int(params["port"]),
        database=params["database"],
        password=params["password"],
        ssl_context=ssl_context,
    )


def sanitize_for_athena(name: str) -> str:
    """
    Lowercase, replace non [a-z0-9_], prefix if starts with digit.
    Do NOT rename reserved words; we rely on quoted identifiers in SQL.
    """
    n = (name or "").strip().lower()
    n = re.sub(r"[^a-z0-9_]", "_", n)
    if re.match(r"^[0-9]", n):
        n = "_" + n
    n = re.sub(r"_+", "_", n).strip("_")
    return n or "col"


def fetch_df(conn: pg8000.native.Connection, sql: str, params: Optional[Iterable[Any]] = None) -> pd.DataFrame:
    """
    Run a SQL query and return a DataFrame.
    Guarantees non-empty, sanitized column names (no more literal "0" column).
    """
    if params is None:
        rows = conn.run(sql)
    else:
        if not isinstance(params, (list, tuple)):
            raise ValueError("--pg-params must be a JSON array for positional params")
        rows = conn.run(sql, tuple(params))

    cols_meta = conn.columns
    norm_names = []
    for i, c in enumerate(cols_meta):
        raw = getattr(c, "name", None)
        name = (str(raw).strip() if raw is not None else "")
        if not name:
            name = f"col_{i}"
        norm_names.append(sanitize_for_athena(name))

    return pd.DataFrame.from_records(rows, columns=norm_names)


# =============================================================================
# Column name alignment & sanitization for Athena/Glue
# =============================================================================
def _esc_lit(s: str) -> str:
    return s.replace("'", "''")


def _esc_ident(s: str) -> str:
    return '"' + s.replace('"', '""') + '"'


def _resolve_relation(conn: pg8000.native.Connection, schema: str, table: str) -> Optional[tuple[str, str]]:
    """Resolve (schema, table) with exact/lower/fuzzy matching via pg_catalog."""
    q_exact = f"""
    SELECT n.nspname, c.relname
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = '{_esc_lit(schema)}' AND c.relname = '{_esc_lit(table)}'
    LIMIT 1
    """
    rows = conn.run(q_exact)
    if rows:
        return rows[0][0], rows[0][1]

    q_lower = f"""
    SELECT n.nspname, c.relname
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = '{_esc_lit(schema.lower())}' AND c.relname = '{_esc_lit(table.lower())}'
    LIMIT 1
    """
    rows = conn.run(q_lower)
    if rows:
        return rows[0][0], rows[0][1]

    q_fuzzy = f"""
    SELECT n.nspname, c.relname
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname ILIKE '{_esc_lit(schema)}' AND c.relname ILIKE '{_esc_lit(table)}'
    ORDER BY n.nspname, c.relname
    LIMIT 1
    """
    rows = conn.run(q_fuzzy)
    if rows:
        return rows[0][0], rows[0][1]
    return None


def fetch_pg_ddl_columns(conn: pg8000.native.Connection, schema: str, table: str) -> List[str]:
    """
    Return column names (ordinal) for a relation (table/view/etc.).
    Try pg_catalog, then information_schema, finally SELECT * LIMIT 0.
    """
    resolved = _resolve_relation(conn, schema, table)
    if resolved is None:
        return []

    s, t = resolved

    q_cat = f"""
    SELECT a.attname
    FROM pg_catalog.pg_attribute a
    JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = '{_esc_lit(s)}'
      AND c.relname = '{_esc_lit(t)}'
      AND a.attnum > 0
      AND NOT a.attisdropped
    ORDER BY a.attnum
    """
    rows = conn.run(q_cat)
    cols = [r[0] for r in rows]
    if cols:
        return cols

    q_info = f"""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = '{_esc_lit(s)}' AND table_name = '{_esc_lit(t)}'
    ORDER BY ordinal_position
    """
    rows = conn.run(q_info)
    cols = [r[0] for r in rows]
    if cols:
        return cols

    q_zero = f"SELECT * FROM {_esc_ident(s)}.{_esc_ident(t)} LIMIT 0"
    conn.run(q_zero)
    cols = [getattr(c, "name", "") for c in conn.columns]
    return [c for c in cols if c]


def fetch_athena_table_columns(
    glue_db: str,
    table: str,
    boto3_session: Optional[boto3.session.Session] = None,
    workgroup: Optional[str] = None,
    s3_output: Optional[str] = None,
) -> List[str]:
    """Return ordered column names from Athena information_schema (ordinal_position)."""
    df = wr.athena.read_sql_query(
        sql=f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = '{glue_db}'
              AND table_name   = '{table}'
            ORDER BY ordinal_position
        """,
        database=glue_db,
        workgroup=workgroup,
        s3_output=s3_output,
        boto3_session=boto3_session,
    )
    return [str(r["column_name"]).strip() for _, r in df.iterrows()]


def align_df_columns_with_pg(
    df: pd.DataFrame,
    ddl_cols: List[str],
    relax: bool = True,
    *,
    prod_cols: Optional[List[str]] = None,   # ordered destination columns (Athena)
    exclude_prod_meta: bool = True           # drop __op/__ts_ms from production target set
) -> pd.DataFrame:
    """
    Align DataFrame columns:
      - If counts match → positional rename to DDL names.
      - If mismatch → align to production columns (if provided), excluding __op/__ts_ms,
        preferring name-based match; else fall back to tail positional (handles SELECT extra, * ...).
      - Preserve any extra computed columns; sanitize all names for Athena.
    """
    df2 = df.copy()
    df_cols = list(df2.columns)

    if not ddl_cols and not prod_cols:
        df2.columns = [sanitize_for_athena(c) for c in df_cols]
        return df2

    # 1) Perfect count match with DDL → positional
    if ddl_cols and len(df_cols) == len(ddl_cols):
        mapping = {df_cols[i]: ddl_cols[i] for i in range(len(df_cols))}
        df2 = df2.rename(columns=mapping)
        df2.columns = [sanitize_for_athena(c) for c in df2.columns]
        return df2

    # 2) Mismatch → choose production target if available
    target_cols = list(prod_cols or ddl_cols or [])
    if exclude_prod_meta:
        target_cols = [c for c in target_cols if c not in ("__op", "__ts_ms")]
    if not target_cols:
        df2.columns = [sanitize_for_athena(c) for c in df2.columns]
        return df2

    target_lower = [c.lower() for c in target_cols]
    df_lower = [c.lower() for c in df_cols]
    t_index: Dict[str, str] = {c.lower(): c for c in target_cols}

    # Strategy A: name-based alignment
    if all(c in set(df_lower) for c in target_lower):
        rename_map: Dict[str, str] = {}
        used = set()
        for col in df_cols:
            lc = col.lower()
            if lc in t_index and lc not in used:
                rename_map[col] = t_index[lc]
                used.add(lc)
        df2 = df2.rename(columns=rename_map)
        df2.columns = [sanitize_for_athena(c) for c in df2.columns]
        return df2

    # Strategy B: tail positional alignment
    n_df, n_tgt = len(df_cols), len(target_cols)
    if n_df >= n_tgt:
        df_tail = df_cols[-n_tgt:]
        mapping = {df_tail[i]: target_cols[i] for i in range(min(len(df_tail), n_tgt))}
        if len(df_tail) != n_tgt:
            print(f"[WARN] Tail alignment mismatch: df_tail={len(df_tail)} vs target={n_tgt}. Using min length.", file=sys.stderr)
        df2 = df2.rename(columns=mapping)
        df2.columns = [sanitize_for_athena(c) for c in df2.columns]
        return df2

    # Strategy C: fewer df cols than target (rare) → head positional
    print(f"[WARN] DataFrame has fewer columns than target (df={n_df}, target={n_tgt}). Aligning first {n_df} by position.", file=sys.stderr)
    mapping = {df_cols[i]: target_cols[i] for i in range(n_df)}
    df2 = df2.rename(columns=mapping)
    df2.columns = [sanitize_for_athena(c) for c in df2.columns]
    return df2


# =============================================================================
# Staging: S3 + Glue table
# =============================================================================
def ensure_database(db: str, boto3_session: Optional[boto3.session.Session] = None) -> None:
    """Ensure Glue database exists (Wrangler if present, else boto3 Glue)."""
    try:
        if hasattr(wr.catalog, "does_database_exist"):
            if not wr.catalog.does_database_exist(db, boto3_session=boto3_session):
                wr.catalog.create_database(name=db, boto3_session=boto3_session)
            return
    except Exception:
        pass

    glue = (boto3_session or boto3.session.Session()).client("glue")
    try:
        glue.get_database(Name=db)
    except glue.exceptions.EntityNotFoundException:
        glue.create_database(DatabaseInput={"Name": db})


def write_staging_parquet(
    df: pd.DataFrame,
    s3_path: str,
    database: str,
    table: str,
    partitions: Optional[List[str]] = None,
    mode: str = "overwrite",
    schema_evolution: bool = True,
    dtype: Optional[Dict[str, str]] = None,
    boto3_session: Optional[boto3.session.Session] = None,
    force_all_string: bool = True,
) -> None:
    """
    Write a Parquet dataset to S3 and register/update a Glue table.

    Defaults:
      - Cast all columns to string (force_all_string=True).
      - Auto-add 'load_date' partition if requested and missing in df.
    """
    ensure_database(database, boto3_session=boto3_session)

    df = df.copy()
    if force_all_string:
        for c in df.columns:
            df[c] = df[c].astype("string")

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if partitions and "load_date" in partitions and "load_date" not in df.columns:
        df["load_date"] = now_utc

    dtype_map = {c: "string" for c in df.columns} if force_all_string else {}
    if dtype:
        dtype_map.update(dtype)

    wr.s3.to_parquet(
        df=df,
        path=s3_path,
        dataset=True,
        database=database,
        table=table,
        partition_cols=partitions or None,
        mode=mode,
        schema_evolution=schema_evolution,
        dtype=dtype_map if dtype_map else None,
        boto3_session=boto3_session,
    )


# =============================================================================
# Athena helpers: production schema, MERGE SQL, MERGE execution
# =============================================================================
def fetch_athena_table_types(
    glue_db: str,
    table: str,
    boto3_session: Optional[boto3.session.Session] = None,
    workgroup: Optional[str] = None,
    s3_output: Optional[str] = None,
) -> Dict[str, str]:
    """
    Return mapping {lower_col_name: data_type} from Athena information_schema.
    Example types: varchar, boolean, integer, bigint, double, decimal(10,2),
                   timestamp, timestamp(6), timestamp with time zone (rare)
    """
    df = wr.athena.read_sql_query(
        sql=f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = '{glue_db}'
              AND table_name   = '{table}'
            ORDER BY ordinal_position
        """,
        database=glue_db,
        workgroup=workgroup,
        s3_output=s3_output,
        boto3_session=boto3_session,
    )
    return {str(r["column_name"]).strip().lower(): str(r["data_type"]).strip().lower() for _, r in df.iterrows()}


def _normalize_ts_type(t: str) -> str:
    """Normalize timestamp-ish types to a CAST-safe target."""
    t = (t or "").lower().strip()
    if t.startswith("timestamp"):
        return "timestamp"
    return t or "varchar"


def _is_integer_type(t: str) -> bool:
    """Return True for integer-like target types."""
    t = (t or "").lower().strip()
    return t in ("integer", "bigint", "smallint")


def _is_decimal_float_type(t: str) -> bool:
    """Return True for decimal/float-like target types."""
    t = (t or "").lower().strip()
    return t.startswith("decimal") or t in ("double", "real", "float")


def _cast_expr_for_col(col: str, target_type: Optional[str], *, op_literal: str = "backfill") -> str:
    """
    Build a safe RHS expression for MERGE that:
      - Injects literals for __op and __ts_ms.
      - Cleans and casts numeric strings (handles NBSP, ID/EN separators, symbols).
      - Normalizes booleans from common string variants.
      - Trims empty strings to NULL for timestamp/date.
      - Falls back to a plain TRY_CAST for other types.

    Notes:
      * For INTEGER-like targets: keep only the leading integer portion (drops fractional part).
      * For DECIMAL/DOUBLE/REAL/FLOAT: normalize to EN format and preserve fractions.
    """
    # Special metadata columns
    if col == "__op":
        tt = (target_type or "varchar")
        lit = (op_literal or "backfill").replace("'", "''")
        return f"CAST('{lit}' AS {tt})"

    if col == "__ts_ms":
        tt = _normalize_ts_type(target_type or "timestamp")
        return (
            "CAST("
            "format_datetime(current_timestamp AT TIME ZONE 'Asia/Jakarta', "
            "'yyyy-MM-dd HH:mm:ss.SSS') AS "
            f"{tt})"
        )

    # Generic path
    safe_col = col.replace('"', '""')
    if not target_type:
        return f's."{safe_col}"'

    t = _normalize_ts_type(target_type)

    # ---------- Numeric cleaning & casting ----------
    if _is_integer_type(t):
        # Base string with NBSP removed
        base = f"TRIM(REPLACE(s.\"{safe_col}\", '\\u00A0',''))"
        # For Indonesian-style numbers: '.' thousands → remove
        id_thousands_removed = f"REPLACE({base}, '.', '')"
        # For English-style numbers: ',' thousands → remove
        en_thousands_removed = f"REPLACE({base}, ',', '')"

        # Strategy:
        # 1) Your proven regex: extract leading integer (drops any decimals).
        # 2) ID format fallback: remove '.' thousands, take part before ',' (decimal), keep digits/sign only.
        # 3) EN format fallback: remove ',' thousands, take part before '.' (decimal), keep digits/sign only.
        return (
            "COALESCE("
            f"TRY_CAST(NULLIF(REGEXP_EXTRACT("
            f"REGEXP_REPLACE({base}, '[^0-9\\-,\\.]', ''), '^-?[0-9]+'), '') AS {t}),"
            f"TRY_CAST(NULLIF(REGEXP_REPLACE(SPLIT_PART({id_thousands_removed}, ',', 1), '[^0-9\\-]', ''), '') AS {t}),"
            f"TRY_CAST(NULLIF(REGEXP_REPLACE(SPLIT_PART({en_thousands_removed}, '.', 1), '[^0-9\\-]', ''), '') AS {t})"
            ")"
        )

    if _is_decimal_float_type(t):
        # Base string with NBSP removed
        base = f"TRIM(REPLACE(s.\"{safe_col}\", '\\u00A0',''))"
        # Normalize ID → EN: remove '.' thousands, convert ',' decimal to '.'
        id_to_en = f"REPLACE(REPLACE({base}, '.', ''), ',', '.')"
        # EN cleanup: remove ',' thousands (keep '.' decimal)
        en_clean = f"REPLACE({base}, ',', '')"

        # Strategy:
        # 1) Try ID→EN normalization → DOUBLE → target type (preserves fraction)
        # 2) Try EN cleanup → DOUBLE → target type
        # 3) Fallback: strip non [0-9.-] and cast directly
        return (
            "COALESCE("
            f"TRY_CAST(TRY_CAST(NULLIF({id_to_en}, '') AS DOUBLE) AS {t}),"
            f"TRY_CAST(TRY_CAST(NULLIF({en_clean}, '') AS DOUBLE) AS {t}),"
            f"TRY_CAST(NULLIF(REGEXP_REPLACE({base}, '[^0-9\\-\\.]', ''), '') AS {t})"
            ")"
        )

    # ---------- Boolean normalization ----------
    if t == "boolean":
        norm = f"LOWER(TRIM(s.\"{safe_col}\"))"
        return (
            f"CASE "
            f"WHEN {norm} IN ('t','true','1','y','yes') THEN TRUE "
            f"WHEN {norm} IN ('f','false','0','n','no') THEN FALSE "
            f"ELSE NULL END"
        )

    # ---------- Timestamps/Dates: trim-empty → NULL, else cast ----------
    if t in ("timestamp", "date"):
        return (
            f"CASE WHEN NULLIF(TRIM(s.\"{safe_col}\"), '') IS NULL THEN NULL "
            f"ELSE TRY_CAST(TRIM(s.\"{safe_col}\") AS {t}) END"
        )

    # ---------- Default path ----------
    return f'TRY_CAST(s."{safe_col}" AS {t})'


def build_merge_sql(
    database: str,
    staging_table: str,
    prod_table: str,
    pk_cols: List[str],
    all_cols: List[str],
    prod_types: Dict[str, str],
    *,
    updated_at_col: Optional[str] = "updated_at",
    op_literal: str = "backfill",
    exclude_update_cols: Optional[List[str]] = None,
    extra_on_predicates: Optional[List[str]] = None,
    extra_set_assignments: Optional[Dict[str, str]] = None,
    extra_insert_assignments: Optional[Dict[str, str]] = None,
    # NEW: source dedup controls
    dedupe_source: bool = True,
    dedupe_tiebreakers: Optional[List[str]] = None,
) -> str:
    """Type-aware MERGE with optional source dedup via ROW_NUMBER().

    Dedup picks exactly one row per PK using ORDER BY:
      1) COALESCE(updated_at, created_at) DESC (when available)
      2) user-provided tie-breakers (DESC)
      3) PK columns (DESC)
    """
    def qident(x: str) -> str:
        return '"' + x.replace('"', '""') + '"'

    def qname(db: str, tbl: str) -> str:
        return f'{qident(db)}.{qident(tbl)}'

    exclude_update_cols = set(exclude_update_cols or [])
    extra_on_predicates = list(extra_on_predicates or [])
    extra_set_assignments = dict(extra_set_assignments or {})
    extra_insert_assignments = dict(extra_insert_assignments or {})
    dedupe_tiebreakers = [c.strip() for c in (dedupe_tiebreakers or []) if c.strip()]

    dest_cols_lower = set(prod_types.keys())

    # Keep only columns that exist in destination (case-insensitive)
    merged_cols = [c for c in all_cols if c.lower() in dest_cols_lower]

    # Add metadata columns if destination has them
    if "__op" in dest_cols_lower and "__op" not in merged_cols:
        merged_cols.append("__op")
    if "__ts_ms" in dest_cols_lower and "__ts_ms" not in merged_cols:
        merged_cols.append("__ts_ms")

    ignored = [c for c in all_cols if c.lower() not in dest_cols_lower]
    if ignored:
        print(f"[INFO] Ignoring staging-only columns not in destination: {ignored}", file=sys.stderr)

    tname = qname(database, prod_table)
    sname = qname(database, staging_table)

    # ON clause with type-aware RHS casting
    on_parts = []
    for pk in pk_cols:
        if pk.lower() not in dest_cols_lower:
            raise RuntimeError(f"Primary key {pk!r} not found in destination schema.")
        dest_type = prod_types.get(pk.lower())
        on_parts.append(f't.{qident(pk)} = {_cast_expr_for_col(pk, dest_type, op_literal=op_literal)}')
    on_parts.extend(extra_on_predicates)
    on_clause = " AND ".join(on_parts) if on_parts else "1=1"

    # UPDATE SET (exclude PKs and explicit exclusions)
    updatable_cols = [c for c in merged_cols if c.lower() in dest_cols_lower and c not in pk_cols and c not in exclude_update_cols]
    rhs_exprs = {c: _cast_expr_for_col(c, prod_types.get(c.lower()), op_literal=op_literal) for c in updatable_cols}
    set_pairs = [f'{qident(c)} = {rhs_exprs[c]}' for c in updatable_cols]

    # Allow custom SET injections
    for col, expr in extra_set_assignments.items():
        if col.lower() in dest_cols_lower:
            set_pairs.append(f'{qident(col)} = {expr}')
        else:
            print(f"[INFO] Skipping extra SET for non-destination column: {col}", file=sys.stderr)

    if not set_pairs:
        # MERGE requires at least one assignment
        set_pairs.append(f'{qident(pk_cols[0])} = t.{qident(pk_cols[0])}')
    set_clause = ", ".join(set_pairs)

    # INSERT: column list + value expressions (type-aware)
    insert_cols = list(merged_cols)
    for col in extra_insert_assignments.keys():
        if col.lower() in dest_cols_lower and col not in insert_cols:
            insert_cols.append(col)

    insert_vals_exprs = []
    for col in insert_cols:
        if col in extra_insert_assignments:
            insert_vals_exprs.append(extra_insert_assignments[col])
        else:
            insert_vals_exprs.append(_cast_expr_for_col(col, prod_types.get(col.lower()), op_literal=op_literal))
    insert_cols_sql = ", ".join(qident(c) for c in insert_cols)
    insert_vals_sql = ", ".join(insert_vals_exprs)

    # WHEN MATCHED guard (only update when source is newer)
    matched_guard = ""
    if updated_at_col and updated_at_col.lower() in dest_cols_lower:
        u_q = qident(updated_at_col)
        target_u = _normalize_ts_type(prod_types.get(updated_at_col.lower(), "timestamp"))
        matched_guard = f"""
      AND COALESCE(
            TRY_CAST(s.{u_q} AS {target_u}),
            TIMESTAMP '1970-01-01 00:00:00'
          ) >
          COALESCE(
            TRY_CAST(t.{u_q} AS {target_u}),
            TIMESTAMP '1970-01-01 00:00:00'
          )
    """

    # USING subquery (with deterministic dedupe if enabled)
    if dedupe_source:
        part_by = ", ".join(f's.{qident(pk)}' for pk in pk_cols)

        order_parts: List[str] = []
        has_updated = updated_at_col and any(updated_at_col.lower() == c.lower() for c in all_cols)
        has_created = any(c.lower() == "created_at" for c in all_cols)
        if has_updated and has_created:
            order_parts.append(f'COALESCE(s.{qident(updated_at_col)}, s."created_at") DESC')
        elif has_updated:
            order_parts.append(f's.{qident(updated_at_col)} DESC')
        elif has_created:
            order_parts.append('s."created_at" DESC')

        for tb in dedupe_tiebreakers:
            order_parts.append(f's.{qident(tb)} DESC')

        for pk in pk_cols:
            order_parts.append(f's.{qident(pk)} DESC')

        order_clause = ", ".join(order_parts) if order_parts else ", ".join(f's.{qident(pk)} DESC' for pk in pk_cols)

        # Project only the columns we need; __op and __ts_ms are injected via SET/VALUES
        proj_cols = ", ".join(f'src.{qident(c)}' for c in insert_cols if c.lower() not in ("__op", "__ts_ms"))
        using_subquery = f"""
            WITH src AS (
              SELECT
                s.*,
                ROW_NUMBER() OVER (
                  PARTITION BY {part_by}
                  ORDER BY {order_clause}
                ) AS rn
              FROM {sname} s
            )
            SELECT {proj_cols}
            FROM src
            WHERE rn = 1
        """.strip()
    else:
        using_subquery = f'SELECT * FROM {sname} s'

    return f"""
            MERGE INTO {tname} t
            USING ({using_subquery}) s
            ON ({on_clause})
            WHEN MATCHED
            {matched_guard}
            THEN UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN INSERT ({insert_cols_sql}) VALUES ({insert_vals_sql})
            """.strip()



def run_athena_sql(
    sql: str,
    database: str,
    s3_output: Optional[str] = None,
    workgroup: Optional[str] = None,
    boto3_session: Optional[boto3.session.Session] = None,
    poll_sec: float = 2.0,
) -> None:
    """Execute DDL/DML (e.g., MERGE/INSERT/CREATE/DROP) in Athena using boto3."""
    ses = boto3_session or boto3.session.Session()
    ath = ses.client("athena")

    req: Dict[str, Any] = {"QueryString": sql, "QueryExecutionContext": {"Database": database}}
    if s3_output:
        req["ResultConfiguration"] = {"OutputLocation": s3_output}
    if workgroup:
        req["WorkGroup"] = workgroup

    resp = ath.start_query_execution(**req)
    qid = resp["QueryExecutionId"]

    import time
    while True:
        r = ath.get_query_execution(QueryExecutionId=qid)
        st = r["QueryExecution"]["Status"]["State"]
        if st in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(poll_sec)

    if st != "SUCCEEDED":
        reason = r["QueryExecution"]["Status"].get("StateChangeReason")
        raise RuntimeError(f"Athena query {qid} ended with state={st}. Reason: {reason}")


# =============================================================================
# Cleanup
# =============================================================================
def drop_staging_table(
    glue_db: str,
    table: str,
    *,
    boto3_session: Optional[boto3.session.Session] = None,
    s3_staging_path: Optional[str] = None,
    purge_objects: bool = False,
    workgroup: Optional[str] = None,
    s3_output: Optional[str] = None,
) -> None:
    """Drop Glue/Athena table `glue_db.table` and (optional) delete S3 objects."""
    try:
        if hasattr(wr.catalog, "delete_table_if_exists"):
            wr.catalog.delete_table_if_exists(
                database=glue_db,
                table=table,
                boto3_session=boto3_session,
            )
        else:
            wr.athena.read_sql_query(
                sql=f'DROP TABLE IF EXISTS "{glue_db}"."{table}"',
                database=glue_db,
                workgroup=workgroup,
                s3_output=s3_output,
                boto3_session=boto3_session,
            )
    except Exception:
        try:
            run_athena_sql(
                sql=f'DROP TABLE IF EXISTS "{glue_db}"."{table}"',
                database=glue_db,
                s3_output=s3_output,
                workgroup=workgroup,
                boto3_session=boto3_session,
            )
        except Exception as e:
            print(f"[WARN] Failed to drop staging table {glue_db}.{table}: {e}", file=sys.stderr)

    if purge_objects and s3_staging_path:
        try:
            wr.s3.delete_objects(
                path=s3_staging_path,
                boto3_session=boto3_session,
            )
        except Exception as e:
            print(f"[WARN] Failed to purge S3 objects at {s3_staging_path}: {e}", file=sys.stderr)


# =============================================================================
# CLI
# =============================================================================
def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="PostgreSQL → Athena staging + Iceberg MERGE (pg8000 + awswrangler + boto3)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    # Modes
    p.add_argument("--mode", choices=["full-run", "stage-only", "merge-only"], required=True)

    # AWS session / Secrets
    p.add_argument("--secret-id", default=None, help="Secrets Manager secret id or ARN for PostgreSQL")
    p.add_argument("--region", default=os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION"), help="AWS region")
    p.add_argument("--profile", default=os.getenv("AWS_PROFILE"), help="AWS CLI profile")

    # PostgreSQL
    p.add_argument("--pg-host", default=None)
    p.add_argument("--pg-port", default=5432, type=int)
    p.add_argument("--pg-user", default=None)
    p.add_argument("--pg-password", default=None)
    p.add_argument("--pg-database", default=None)
    p.add_argument("--sslmode", "--pg-sslmode", dest="pg_sslmode", default=None)
    p.add_argument("--ssl-no-verify", action="store_true")
    p.add_argument("--pg-sql", default=None, help="SQL to extract from PostgreSQL")
    p.add_argument("--pg-params", default=None, help="JSON array for positional params (e.g. [1, \"abc\"])")

    # Alignment with PG DDL
    p.add_argument("--align-ddl", action="store_true", help="Rename columns to match PostgreSQL DDL order & names")
    p.add_argument("--pg-schema", default=None, help="PostgreSQL schema for DDL (e.g. public)")
    p.add_argument("--pg-table", default=None, help="PostgreSQL table name for DDL alignment")
    p.add_argument("--relax-align", action="store_true", help="Allow partial mapping if column counts differ")

    # Staging (S3 + Glue)
    p.add_argument("--s3-staging-path", required=False, help="s3://bucket/prefix/ for Parquet dataset")
    p.add_argument("--glue-db", required=True, help="Glue/Athena database name")
    p.add_argument("--staging-table", required=True, help="Staging table name (Glue Catalog)")
    p.add_argument("--partitions", default=None, help="Comma-separated partition columns (e.g. load_date)")
    p.add_argument("--write-mode", default="overwrite_partitions", choices=["overwrite_partitions", "append"])

    # Merge (Athena)
    p.add_argument("--prod-table", required=False, help="Production (Iceberg) table name")
    p.add_argument("--pk", required=False, help="Comma-separated primary key columns")
    p.add_argument("--athena-output", required=True, help="s3://bucket/query-results/ for Athena")
    p.add_argument("--workgroup", default=None, help="Athena workgroup")

    # MERGE options
    p.add_argument("--updated-at-col", default=None, help="Column used as update guard; empty/None disables.")
    p.add_argument("--op-literal", default="backfill", help="Literal to populate __op during MERGE.")
    p.add_argument("--exclude-update-cols", default="", help="Comma-separated columns to exclude from UPDATE.")
    p.add_argument("--extra-on", default="", help="Extra ON predicates joined by AND.")

    # Cleanup options
    p.add_argument("--drop-staging-after-merge", action="store_true")
    p.add_argument("--purge-staging-objects", action="store_true")

    # Deduplication
    p.add_argument("--dedupe-source", action="store_true", default=True, help="Dedupe source rows by PK using ROW_NUMBER() = 1 before MERGE.")
    p.add_argument("--dedupe-tiebreakers", default="", help="Comma-separated extra tie-breakers after updated_at/created_at, e.g. 'event_time,ingest_ts,id'.")


    # Misc
    p.add_argument("--debug", action="store_true")

    return p.parse_args(argv)


# =============================================================================
# Main
# =============================================================================
def main(argv=None) -> int:
    args = parse_args(argv)

    exclude_update_cols = [c.strip() for c in (args.exclude_update_cols.split(",") if args.exclude_update_cols else []) if c.strip()]
    extra_on_preds = [p.strip() for p in (args.extra_on.split(" AND ") if args.extra_on else []) if p.strip()]

    boto3_ses = _get_boto3_session(args.region, args.profile)

    if args.debug:
        print(
            f"[DEBUG] mode={args.mode} region={args.region!r} profile={args.profile!r} "
            f"sslmode={args.pg_sslmode!r} ssl_no_verify={args.ssl_no_verify}",
            file=sys.stderr,
        )

    # Parse optional positional params for PG
    pg_params = None
    if args.pg_params:
        try:
            parsed = json.loads(args.pg_params)
            if not isinstance(parsed, list):
                raise ValueError("--pg-params must be a JSON array")
            pg_params = parsed
        except Exception as exc:
            print(f"[ERROR] --pg-params invalid: {exc}", file=sys.stderr)
            return 2

    df: Optional[pd.DataFrame] = None

    # Extract & Stage
    if args.mode in ("full-run", "stage-only"):
        if not args.pg_sql:
            print("[ERROR] --pg-sql is required for stage/full-run", file=sys.stderr)
            return 3
        if not args.s3_staging_path:
            print("[ERROR] --s3-staging-path is required for stage/full-run", file=sys.stderr)
            return 3

        # Connect + fetch
        try:
            conn = connect_postgres_pg8000(
                secret_id=args.secret_id,
                region=args.region,
                profile=args.profile,
                host=args.pg_host,
                port=args.pg_port,
                user=args.pg_user,
                password=args.pg_password,
                database=args.pg_database,
                sslmode=args.pg_sslmode,
                ssl_no_verify=args.ssl_no_verify,
            )
        except Exception as e:
            print(f"[ERROR] PostgreSQL connection failed: {e}", file=sys.stderr)
            return 4

        try:
            df = fetch_df(conn, args.pg_sql, params=pg_params)

            # Optional: align columns
            if args.align_ddl:
                if not args.pg_schema or not args.pg_table:
                    print("[ERROR] --align-ddl requires --pg-schema and --pg-table", file=sys.stderr)
                    return 9

                ddl_cols = fetch_pg_ddl_columns(conn, args.pg_schema, args.pg_table)
                prod_cols = []
                if args.prod_table:
                    try:
                        prod_cols = fetch_athena_table_columns(
                            glue_db=args.glue_db,
                            table=args.prod_table,
                            boto3_session=boto3_ses,
                            workgroup=args.workgroup,
                            s3_output=args.athena_output,
                        )
                    except Exception as e:
                        print(f"[WARN] Failed to fetch production columns for alignment: {e}", file=sys.stderr)

                if args.debug:
                    print(f"[DEBUG] DDL columns ({len(ddl_cols)}): {ddl_cols}", file=sys.stderr)
                    if prod_cols:
                        print(f"[DEBUG] PROD columns ({len(prod_cols)}): {prod_cols}", file=sys.stderr)

                df = align_df_columns_with_pg(
                    df,
                    ddl_cols=ddl_cols,
                    relax=True,
                    prod_cols=prod_cols or None,
                    exclude_prod_meta=True,
                )

        finally:
            try:
                conn.close()
            except Exception:
                pass

        if args.debug:
            print(f"[DEBUG] extracted rows={len(df)} cols={list(df.columns)}", file=sys.stderr)

        # Write to S3 + Glue
        parts = [c.strip() for c in (args.partitions.split(",") if args.partitions else []) if c.strip()]
        try:
            write_staging_parquet(
                df=df,
                s3_path=args.s3_staging_path,
                database=args.glue_db,
                table=args.staging_table,
                partitions=parts if parts else None,
                mode=args.write_mode,
                schema_evolution=True,
                boto3_session=boto3_ses,
                force_all_string=True,
            )
        except Exception as e:
            print(f"[ERROR] Failed writing staging parquet/catalog: {e}", file=sys.stderr)
            return 5

    # Merge
    if args.mode in ("full-run", "merge-only"):
        if not args.prod_table or not args.pk:
            print("[ERROR] --prod-table and --pk are required for merge/full-run", file=sys.stderr)
            return 6

        # Discover staging columns
        if df is None:
            try:
                preview = wr.athena.read_sql_query(
                    sql=f'SELECT * FROM "{args.glue_db}"."{args.staging_table}" LIMIT 1',
                    database=args.glue_db,
                    s3_output=args.athena_output,
                    workgroup=args.workgroup,
                    boto3_session=boto3_ses,
                )
                cols = list(preview.columns)
            except Exception as e:
                print(f"[ERROR] Unable to infer columns from staging: {e}", file=sys.stderr)
                return 7
        else:
            cols = list(df.columns)

        # Fetch production column types
        prod_types = fetch_athena_table_types(
            glue_db=args.glue_db,
            table=args.prod_table,
            boto3_session=boto3_ses,
            workgroup=args.workgroup,
            s3_output=args.athena_output,
        )
        if args.debug:
            print(f"[DEBUG] prod_types: {prod_types}", file=sys.stderr)

        pk_cols = [sanitize_for_athena(c.strip()) for c in args.pk.split(",") if c.strip()]

        merge_sql = build_merge_sql(
            database=args.glue_db,
            staging_table=args.staging_table,
            prod_table=args.prod_table,
            pk_cols=pk_cols,
            all_cols=cols,
            prod_types=prod_types,
            updated_at_col=(args.updated_at_col or None),
            op_literal=args.op_literal,
            exclude_update_cols=exclude_update_cols,
            extra_on_predicates=extra_on_preds,
            dedupe_source=args.dedupe_source,
            dedupe_tiebreakers=[c.strip() for c in (args.dedupe_tiebreakers.split(",") if args.dedupe_tiebreakers else []) if c.strip()],
        )


        if args.debug:
            print("[DEBUG] MERGE SQL:\n" + merge_sql, file=sys.stderr)

        try:
            run_athena_sql(
                sql=merge_sql,
                database=args.glue_db,
                s3_output=args.athena_output,
                workgroup=args.workgroup,
                boto3_session=boto3_ses,
            )
        except Exception as e:
            print(f"[ERROR] Athena MERGE failed: {e}", file=sys.stderr)
            return 8

        if args.drop_staging_after_merge:
            drop_staging_table(
                glue_db=args.glue_db,
                table=args.staging_table,
                boto3_session=boto3_ses,
                s3_staging_path=args.s3_staging_path,
                purge_objects=args.purge_staging_objects,
                workgroup=args.workgroup,
                s3_output=args.athena_output,
            )

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
