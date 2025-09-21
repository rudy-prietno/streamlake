#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Athena → Athena backfill (type-aware MERGE) with boto3 + awswrangler.

Credential priority:
1) Explicit keys (CLI)
2) Profile (CLI)  [with optional files override]
3) Default (instance role / env)

Features:
- No `catalog=` kwarg to awswrangler (compat with older versions)
- Qualify information_schema with catalog inside SQL
- Flexible source/destination catalogs (can be blank to omit)
- Avoid double-qualification in USING
- De-duplicate PKs
- Metadata __op & __ts_ms (Asia/Jakarta)
- Type-aware normalization for numeric/boolean/timestamp/date
- OPTIONAL range filter for USING subquery: --filter-col/--start-ts/--end-ts
"""

from __future__ import annotations

import os
import sys
import time
import argparse
from typing import Any, Dict, List, Optional

import boto3
import awswrangler as wr


# ---------------------------------------------------------------------------
# Quoting & fully-qualified name helpers
# ---------------------------------------------------------------------------
def qident(x: str) -> str:
    return '"' + x.replace('"', '""') + '"'


def _normalize_catalog(cat: Optional[str]) -> Optional[str]:
    if cat is None:
        return None
    cat = str(cat).strip()
    return cat or None


def fqname(catalog: Optional[str], schema: str, table: str) -> str:
    catalog = _normalize_catalog(catalog)
    if "." in table:
        s, t = table.split(".", 1)
        if catalog:
            return f'{qident(catalog)}.{qident(s)}.{qident(t)}'
        return f'{qident(s)}.{qident(t)}'
    if catalog:
        return f'{qident(catalog)}.{qident(schema)}.{qident(table)}'
    return f'{qident(schema)}.{qident(table)}'


# ---------------------------------------------------------------------------
# Athena schema helpers (no `catalog=` kwarg; catalog qualified inside SQL)
# ---------------------------------------------------------------------------
def fetch_athena_table_types(
    glue_db: str,
    table: str,
    *,
    catalog: Optional[str] = None,
    boto3_session: Optional[boto3.session.Session] = None,
    workgroup: Optional[str] = None,
    s3_output: Optional[str] = None,
) -> Dict[str, str]:
    catalog = _normalize_catalog(catalog)
    cat_prefix = f"{qident(catalog)}." if catalog else ""
    sql = f"""
        SELECT column_name, data_type
        FROM {cat_prefix}information_schema.columns
        WHERE table_schema = '{glue_db}'
          AND table_name   = '{table}'
        ORDER BY ordinal_position
    """
    df = wr.athena.read_sql_query(
        sql=sql,
        database=glue_db,  # required by wrangler
        workgroup=workgroup,
        s3_output=s3_output,
        boto3_session=boto3_session,
    )
    return {
        str(r["column_name"]).strip().lower(): str(r["data_type"]).strip().lower()
        for _, r in df.iterrows()
    }


def fetch_athena_table_columns(
    glue_db: str,
    table: str,
    *,
    catalog: Optional[str] = None,
    boto3_session: Optional[boto3.session.Session] = None,
    workgroup: Optional[str] = None,
    s3_output: Optional[str] = None,
) -> List[str]:
    catalog = _normalize_catalog(catalog)
    cat_prefix = f"{qident(catalog)}." if catalog else ""
    sql = f"""
        SELECT column_name
        FROM {cat_prefix}information_schema.columns
        WHERE table_schema = '{glue_db}'
          AND table_name   = '{table}'
        ORDER BY ordinal_position
    """
    df = wr.athena.read_sql_query(
        sql=sql,
        database=glue_db,
        workgroup=workgroup,
        s3_output=s3_output,
        boto3_session=boto3_session,
    )
    return [str(c).strip() for c in df["column_name"].tolist()]


# ---------------------------------------------------------------------------
# Casting helpers (type-aware normalization)
# ---------------------------------------------------------------------------
def _normalize_ts_type(t: str) -> str:
    t = (t or "").lower().strip()
    if t.startswith("timestamp"):
        return "timestamp"
    return t or "string"


def _is_int_type(t: str) -> bool:
    t = (t or "").lower()
    return t in {"tinyint", "smallint", "integer", "int", "bigint"}


def _is_float_type(t: str) -> bool:
    t = (t or "").lower()
    return t in {"double", "real", "float", "double precision"}


def _is_decimal_type(t: str) -> bool:
    t = (t or "").lower()
    return t.startswith("decimal") or t.startswith("numeric")


def _is_boolean_type(t: str) -> bool:
    t = (t or "").lower()
    return t in {"boolean", "bool"}


def _is_date_type(t: str) -> bool:
    t = (t or "").lower()
    return t == "date"


def _is_timestamp_type(t: str) -> bool:
    return _normalize_ts_type(t) == "timestamp"


def _is_stringish_type(t: str) -> bool:
    t = (t or "").lower()
    return t.startswith("varchar") or t.startswith("char") or t == "string"


# --- NEW: timestamp literal helper ---
def _ts_literal(ts_str: str) -> str:
    safe = (ts_str or "").replace("'", "''").strip()
    if not safe:
        raise ValueError("Empty timestamp literal")
    return f"TIMESTAMP '{safe}'"


def _numeric_parse_expr(col: str, target_type: str) -> str:
    """
    Returns a COALESCE(...) expression that:
      1) strips Unicode spaces, removes non [0-9,.-] for a direct parse (keeps first run)
      2) fallback: remove dots, split by comma, remove non-digits/- and parse
      3) fallback: remove commas, split by dot, remove non-digits/- and parse
    All string ops are on CAST(... AS varchar) to avoid REPLACE(integer, ...) errors.
    """
    c = col.replace('"', '""')
    # Path 1
    p1 = (
        "TRY_CAST("
        "NULLIF("
        "REGEXP_EXTRACT("
        "REGEXP_REPLACE("
        f"TRIM(REGEXP_REPLACE(CAST(s.\"{c}\" AS varchar), '\\\\p{{Zs}}', '')),"
        "'[^0-9\\-,\\.]', ''"
        "), '^-?[0-9]+'"
        "), ''"
        f") AS {target_type})"
    )
    # Path 2
    p2 = (
        "TRY_CAST("
        "NULLIF("
        "REGEXP_REPLACE("
        "SPLIT_PART("
        f"REGEXP_REPLACE(TRIM(CAST(s.\"{c}\" AS varchar)), '\\\\.', ''),"
        "',', 1"
        "), '[^0-9\\-]', ''"
        "), ''"
        f") AS {target_type})"
    )
    # Path 3
    p3 = (
        "TRY_CAST("
        "NULLIF("
        "REGEXP_REPLACE("
        "SPLIT_PART("
        f"REGEXP_REPLACE(TRIM(CAST(s.\"{c}\" AS varchar)), ',', ''),"
        "'.', 1"
        "), '[^0-9\\-]', ''"
        "), ''"
        f") AS {target_type})"
    )
    return f"COALESCE({p1}, {p2}, {p3})"


def _boolean_parse_expr(col: str) -> str:
    c = col.replace('"', '""')
    v = f"LOWER(TRIM(CAST(s.\"{c}\" AS varchar)))"
    return (
        "CASE "
        f"WHEN {v} IN ('t','true','1','y','yes') THEN TRUE "
        f"WHEN {v} IN ('f','false','0','n','no') THEN FALSE "
        "ELSE NULL END"
    )


def _nullable_trim_try_cast(col: str, target_type: str) -> str:
    c = col.replace('"', '""')
    return (
        "CASE "
        f"WHEN NULLIF(TRIM(CAST(s.\"{c}\" AS varchar)), '') IS NULL THEN NULL "
        f"ELSE TRY_CAST(TRIM(CAST(s.\"{c}\" AS varchar)) AS {target_type}) "
        "END"
    )


def _cast_expr_for_col(col: str, target_type: Optional[str], *, op_literal: str = "backfill") -> str:
    """
    Build a source expression for column `col` shaped to the destination `target_type`.
    Handles metadata (__op/__ts_ms), numerics, booleans, date/time, and strings.
    """
    # Metadata
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

    # No target type known -> passthrough
    if not target_type:
        safe_col = col.replace('"', '""')
        return f's."{safe_col}"'

    tt = target_type.strip().lower()

    # Numerics
    if _is_int_type(tt) or _is_decimal_type(tt) or _is_float_type(tt):
        return _numeric_parse_expr(col, tt)

    # Booleans
    if _is_boolean_type(tt):
        return _boolean_parse_expr(col)

    # Timestamps / Date
    if _is_timestamp_type(tt) or _is_date_type(tt):
        return _nullable_trim_try_cast(col, tt)

    # Strings / other
    if _is_stringish_type(tt):
        safe_col = col.replace('"', '""')
        return f"TRY_CAST(s.\"{safe_col}\" AS {tt})"

    # Default conservative behavior
    safe_col = col.replace('"', '""')
    return f"TRY_CAST(s.\"{safe_col}\" AS {tt})"


# ---------------------------------------------------------------------------
# MERGE builder
# ---------------------------------------------------------------------------
def build_merge_sql(
    *,
    source_catalog: Optional[str],
    source_db: str,
    source_table: str,
    dest_catalog: Optional[str],
    dest_db: str,
    dest_table: str,
    pk_cols: List[str],
    all_cols: List[str],
    dest_types: Dict[str, str],
    updated_at_col: Optional[str] = "updated_at",
    op_literal: str = "backfill",
    exclude_update_cols: Optional[List[str]] = None,
    extra_on_predicates: Optional[List[str]] = None,
    extra_set_assignments: Optional[Dict[str, str]] = None,
    extra_insert_assignments: Optional[Dict[str, str]] = None,
    # --- NEW: range filter ---
    filter_col: Optional[str] = None,
    filter_start_ts: Optional[str] = None,
    filter_end_ts: Optional[str] = None,
) -> str:
    exclude_update_cols = set(exclude_update_cols or [])
    extra_on_predicates = list(extra_on_predicates or [])
    extra_set_assignments = dict(extra_set_assignments or {})
    extra_insert_assignments = dict(extra_insert_assignments or {})

    dest_cols_lower = set(dest_types.keys())
    src_cols_lower = {str(c).lower() for c in all_cols}

    merged_cols = [c for c in all_cols if c.lower() in dest_cols_lower]
    if "__op" in dest_cols_lower and "__op" not in merged_cols:
        merged_cols.append("__op")
    if "__ts_ms" in dest_cols_lower and "__ts_ms" not in merged_cols:
        merged_cols.append("__ts_ms")

    ignored = [c for c in all_cols if c.lower() not in dest_cols_lower]
    if ignored:
        print(f"[INFO] Ignoring staging-only columns not in destination: {ignored}", file=sys.stderr)

    tname = fqname(dest_catalog, dest_db, dest_table)
    sname = fqname(source_catalog, source_db, source_table)

    # ON clause (PKs) — type-aware
    on_parts = []
    for pk in pk_cols:
        if pk.lower() not in dest_cols_lower:
            raise RuntimeError(f"Primary key {pk!r} not found in destination schema.")
        dest_type = dest_types.get(pk.lower())
        on_parts.append(f't.{qident(pk)} = {_cast_expr_for_col(pk, dest_type, op_literal=op_literal)}')
    on_parts.extend(extra_on_predicates)
    on_clause = " AND ".join(on_parts) if on_parts else "1=1"

    # USING subquery with optional range filter
    # Priority:
    # (1) If start/end provided -> use range on filter_col (or updated_at_col if filter_col None)
    # (2) Else if updated_at exists -> 30-day guard
    # (3) Else -> SELECT * without filter
    eff_filter_col = (filter_col or updated_at_col)
    has_eff_col = bool(eff_filter_col) and (eff_filter_col.lower() in src_cols_lower)

    def _mk_range_where(col: str) -> str:
        tgt = _normalize_ts_type(dest_types.get(col.lower(), "timestamp"))
        conds = []
        if filter_start_ts:
            conds.append(f"TRY_CAST({qident(col)} AS {tgt}) >= {_ts_literal(filter_start_ts)}")
        if filter_end_ts:
            conds.append(f"TRY_CAST({qident(col)} AS {tgt}) < {_ts_literal(filter_end_ts)}")
        return " AND ".join(conds) if conds else "1=1"

    if (filter_start_ts or filter_end_ts) and has_eff_col:
        where_clause = _mk_range_where(eff_filter_col)
        using_subquery = f"SELECT * FROM {sname} WHERE {where_clause}"
    elif (filter_start_ts or filter_end_ts) and not has_eff_col:
        print(f"[WARN] Filter column '{eff_filter_col}' not found in source; ignoring start/end filter.", file=sys.stderr)
        using_subquery = f"SELECT * FROM {sname}"
    else:
        has_updated = bool(updated_at_col) and (updated_at_col.lower() in src_cols_lower)
        if has_updated:
            target_u = _normalize_ts_type(dest_types.get(updated_at_col.lower(), "timestamp"))
            using_subquery = f"""
            SELECT *
            FROM {sname}
            WHERE COALESCE(
                    TRY_CAST({qident(updated_at_col)} AS {target_u}),
                    TIMESTAMP '1970-01-01 00:00:00'
                  ) >
                  CAST(date_add('day', -30, current_date) AS timestamp)
            """.strip()
        else:
            using_subquery = f"SELECT * FROM {sname}"

    # UPDATE clause (skip PKs / excluded)
    updatable_cols = [
        c for c in merged_cols
        if c.lower() in dest_cols_lower and c not in pk_cols and c not in exclude_update_cols
    ]
    rhs_exprs = {c: _cast_expr_for_col(c, dest_types.get(c.lower()), op_literal=op_literal) for c in updatable_cols}
    set_pairs = [f'{qident(c)} = {rhs_exprs[c]}' for c in updatable_cols]

    for col, expr in extra_set_assignments.items():
        if col.lower() in dest_cols_lower:
            set_pairs.append(f'{qident(col)} = {expr}')
        else:
            print(f"[INFO] Skipping extra SET for non-destination column: {col}", file=sys.stderr)

    if not set_pairs:
        set_pairs.append(f'{qident(pk_cols[0])} = t.{qident(pk_cols[0])}')
    set_clause = ", ".join(set_pairs)

    # INSERT columns / values
    insert_cols = list(merged_cols)
    for col in extra_insert_assignments.keys():
        if col.lower() in dest_cols_lower and col not in insert_cols:
            insert_cols.append(col)

    insert_vals_exprs = []
    for col in insert_cols:
        if col in extra_insert_assignments:
            insert_vals_exprs.append(extra_insert_assignments[col])
        else:
            insert_vals_exprs.append(_cast_expr_for_col(col, dest_types.get(col.lower()), op_literal=op_literal))
    insert_cols_sql = ", ".join(qident(c) for c in insert_cols)
    insert_vals_sql = ", ".join(insert_vals_exprs)

    # WHEN MATCHED guard (only if dest has the updated column)
    matched_guard = ""
    if updated_at_col and updated_at_col.lower() in dest_cols_lower:
        u_q = qident(updated_at_col)
        target_u = _normalize_ts_type(dest_types.get(updated_at_col.lower(), "timestamp"))
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

    return f"""
    MERGE INTO {tname} t
    USING ({using_subquery}) s
      ON ({on_clause})
    WHEN MATCHED
      {matched_guard}
    THEN UPDATE SET {set_clause}
    WHEN NOT MATCHED THEN INSERT ({insert_cols_sql}) VALUES ({insert_vals_sql})
    """.strip()


# ---------------------------------------------------------------------------
# Athena execution
# ---------------------------------------------------------------------------
def run_athena_sql(
    *,
    sql: str,
    database: str,
    catalog: Optional[str] = None,
    s3_output: Optional[str] = None,
    workgroup: Optional[str] = None,
    boto3_session: Optional[boto3.session.Session] = None,
    poll_sec: float = 2.0,
) -> None:
    ses = boto3_session or boto3.session.Session()
    ath = ses.client("athena")

    qctx: Dict[str, Any] = {"Database": database}
    cat_norm = _normalize_catalog(catalog)
    if cat_norm:
        qctx["Catalog"] = cat_norm

    req: Dict[str, Any] = {"QueryString": sql, "QueryExecutionContext": qctx}
    if s3_output:
        req["ResultConfiguration"] = {"OutputLocation": s3_output}
    if workgroup:
        req["WorkGroup"] = workgroup

    resp = ath.start_query_execution(**req)
    qid = resp["QueryExecutionId"]

    while True:
        r = ath.get_query_execution(QueryExecutionId=qid)
        st = r["QueryExecution"]["Status"]["State"]
        if st in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(poll_sec)

    if st != "SUCCEEDED":
        reason = r["QueryExecution"]["Status"].get("StateChangeReason")
        raise RuntimeError(f"Athena query {qid} ended with state={st}. Reason: {reason}")


# ---------------------------------------------------------------------------
# Credential utilities
# ---------------------------------------------------------------------------
def _assert_session_has_credentials(session: boto3.session.Session, label: str = "boto3_session") -> None:
    try:
        sts = session.client("sts")
        sts.get_caller_identity()
    except Exception as e:
        raise RuntimeError(
            f"[FATAL] No AWS credentials available for {label}. "
            f"Ensure explicit keys, or --profile, or instance role is present. "
            f"Original error: {e}"
        )


def _make_boto3_session(
    *,
    region: str,
    profile: Optional[str],
    access_key: Optional[str],
    secret_key: Optional[str],
    session_token: Optional[str],
    shared_credentials_file: Optional[str],
    config_file: Optional[str],
) -> boto3.session.Session:
    if shared_credentials_file:
        os.environ["AWS_SHARED_CREDENTIALS_FILE"] = shared_credentials_file
    if config_file:
        os.environ["AWS_CONFIG_FILE"] = config_file

    if access_key and secret_key:
        ses = boto3.session.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            aws_session_token=session_token,
            region_name=region,
        )
        _assert_session_has_credentials(ses, label="explicit-keys")
        return ses

    if profile:
        ses = boto3.session.Session(region_name=region, profile_name=profile)
        _assert_session_has_credentials(ses, label=f"profile={profile}")
        return ses

    ses = boto3.session.Session(region_name=region)
    _assert_session_has_credentials(ses, label="default")
    return ses


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def _dedupe_preserve_order(xs: List[str]) -> List[str]:
    seen = set()
    out = []
    for x in xs:
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out


def build_and_run_merge(
    *,
    region: str,
    profile: Optional[str],
    workgroup: Optional[str],
    s3_output: Optional[str],
    source_catalog: Optional[str],
    source_db: str,
    source_table: str,
    dest_catalog: Optional[str],
    dest_db: str,
    dest_table: str,
    pk_cols: List[str],
    updated_col: Optional[str],
    op_literal: str,
    exclude_update_cols: Optional[List[str]],
    # explicit credentials
    aws_access_key_id: Optional[str],
    aws_secret_access_key: Optional[str],
    aws_session_token: Optional[str],
    shared_credentials_file: Optional[str],
    config_file: Optional[str],
    # --- NEW: filter controls ---
    filter_col: Optional[str],
    filter_start_ts: Optional[str],
    filter_end_ts: Optional[str],
) -> None:
    """
    - Create session (explicit keys > profile > default/IMDS)
    - Read destination types + source columns
    - Build MERGE SQL
    - Execute MERGE
    """
    session = _make_boto3_session(
        region=region,
        profile=profile,
        access_key=aws_access_key_id,
        secret_key=aws_secret_access_key,
        session_token=aws_session_token,
        shared_credentials_file=shared_credentials_file,
        config_file=config_file,
    )

    pk_cols = _dedupe_preserve_order([p.strip() for p in pk_cols if p and p.strip()])

    dest_types = fetch_athena_table_types(
        glue_db=dest_db,
        table=dest_table,
        catalog=dest_catalog,
        boto3_session=session,
        workgroup=workgroup,
        s3_output=s3_output,
    )
    if not dest_types:
        raise RuntimeError(f"Destination schema not found: {dest_db}.{dest_table} (catalog={dest_catalog or 'None'})")

    src_cols = fetch_athena_table_columns(
        glue_db=source_db,
        table=source_table,
        catalog=source_catalog,
        boto3_session=session,
        workgroup=workgroup,
        s3_output=s3_output,
    )
    if not src_cols:
        raise RuntimeError(f"Source schema not found: {source_db}.{source_table} (catalog={source_catalog or 'None'})")

    candidate_cols = list(src_cols) + ["__op", "__ts_ms"]

    merge_sql = build_merge_sql(
        source_catalog=source_catalog,
        source_db=source_db,
        source_table=source_table,
        dest_catalog=dest_catalog,
        dest_db=dest_db,
        dest_table=dest_table,
        pk_cols=pk_cols,
        all_cols=candidate_cols,
        dest_types=dest_types,
        updated_at_col=updated_col,
        op_literal=op_literal,
        exclude_update_cols=exclude_update_cols or [],
        extra_on_predicates=[],
        extra_set_assignments={},
        extra_insert_assignments={},
        # NEW
        filter_col=filter_col,
        filter_start_ts=filter_start_ts,
        filter_end_ts=filter_end_ts,
    )

    print("[INFO] MERGE SQL to execute:\n", merge_sql, file=sys.stderr)

    run_athena_sql(
        sql=merge_sql,
        database=dest_db,
        catalog=dest_catalog,
        s3_output=s3_output,
        workgroup=workgroup,
        boto3_session=session,
    )
    print("[OK] MERGE succeeded.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Athena→Athena backfill MERGE (type-aware).")
    p.add_argument("--region", default="ap-southeast-1", help="AWS region for boto3 session.")
    p.add_argument("--profile", default=None, help="AWS profile name (optional).")
    p.add_argument("--workgroup", default=None, help="Athena workgroup to use.")
    p.add_argument("--s3-output", default=None, help="S3 path for Athena query results if workgroup not configured.")

    # Optional explicit credentials (Access Key / Secret Key)
    p.add_argument("--aws-access-key-id", default=None, help="Explicit AWS access key id (optional).")
    p.add_argument("--aws-secret-access-key", default=None, help="Explicit AWS secret access key (optional).")
    p.add_argument("--aws-session-token", default=None, help="Explicit AWS session token (optional).")

    # Optional config paths (helpful when using sudo)
    p.add_argument("--shared-credentials-file", default=None, help="Path to shared credentials file (~/.aws/credentials).")
    p.add_argument("--config-file", default=None, help="Path to AWS config file (~/.aws/config).")

    # SOURCE (allow blank catalog to omit prefix)
    p.add_argument("--source-catalog", default="AwsDataCatalog", help="Athena catalog for SOURCE. Use '' to omit.")
    p.add_argument("--source-db", default="prod_loaders", help="SOURCE Glue database (schema).")
    p.add_argument("--source-table", default="backfill_settlement_details", help="SOURCE table.")

    # DESTINATION (allow blank catalog to omit prefix)
    p.add_argument("--dest-catalog", default="AwsDataCatalog", help="Athena catalog for DESTINATION. Use '' to omit.")
    p.add_argument("--dest-db", default="prod_silver", help="DEST Glue database (schema).")
    p.add_argument("--dest-table", default="settlement_details", help="DEST table.")

    # PK and behaviors
    p.add_argument("--pk", action="append", default=None, help="Primary key column(s). Can be used multiple times.")
    p.add_argument("--updated-col", default="updated_at", help="Updated-at column for WHEN MATCHED guard (or '' to disable).")
    p.add_argument("--op-literal", default="backfill", help="Value for __op metadata column (if present in destination).")
    p.add_argument("--exclude-update", action="append", default=[], help="Columns to exclude from UPDATE (can repeat).")

    # --- NEW: filter options ---
    p.add_argument("--filter-col", default=None,
                   help="Nama kolom sumber untuk filter range. Default: pakai --updated-col jika diisi.")
    p.add_argument("--start-ts", default=None,
                   help="Timestamp awal (inclusive), format 'YYYY-MM-DD' atau 'YYYY-MM-DD HH:MM:SS'.")
    p.add_argument("--end-ts", default=None,
                   help="Timestamp akhir (exclusive), format 'YYYY-MM-DD' atau 'YYYY-MM-DD HH:MM:SS'.")

    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)
    updated_col = args.updated_col if args.updated_col else None
    pk_cols = args.pk if args.pk else ["id"]

    build_and_run_merge(
        region=args.region,
        profile=args.profile,
        workgroup=args.workgroup,
        s3_output=args.s3_output,
        source_catalog=args.source_catalog,
        source_db=args.source_db,
        source_table=args.source_table,
        dest_catalog=args.dest_catalog,
        dest_db=args.dest_db,
        dest_table=args.dest_table,
        pk_cols=pk_cols,
        updated_col=updated_col,
        op_literal=args.op_literal,
        exclude_update_cols=args.exclude_update,
        aws_access_key_id=args.aws_access_key_id,
        aws_secret_access_key=args.aws_secret_access_key,
        aws_session_token=args.aws_session_token,
        shared_credentials_file=args.shared_credentials_file,
        config_file=args.config_file,
        # NEW
        filter_col=args.filter_col,
        filter_start_ts=args.start_ts,
        filter_end_ts=args.end_ts,
    )


if __name__ == "__main__":
    main()
