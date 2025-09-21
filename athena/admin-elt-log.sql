CREATE EXTERNAL TABLE IF NOT EXISTS glue-database-catalog.monitoring.etl_batch_runner_log_raw (
  line string
)
PARTITIONED BY (
  system string,
  job    string,
  year   int,
  month  int,
  day    int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('serialization.format'='1')
LOCATION 's3://bucket-name/logs/cron-batch/'
TBLPROPERTIES (
  'hive.recursive.directories'='true',
  'projection.enabled'='true',

  'projection.system.type'='enum',
  'projection.system.values'='database-name-1,database-name-2,database-name-2', --replace with your database name source

  'projection.job.type'='injected',

  'projection.year.type'='integer',
  'projection.year.range'='2020,2100',

  'projection.month.type'='integer',
  'projection.month.range'='1,12',
  'projection.month.digits'='2',

  'projection.day.type'='integer',
  'projection.day.range'='1,31',
  'projection.day.digits'='2',

  'storage.location.template'='s3://bucket-name/logs/cron-batch/${system}/${job}/${year}/${month}/${day}/'
);



CREATE OR REPLACE VIEW glue-database-catalog.view_etl_batch_runner_raw AS
SELECT
  t.system,
  t.job,
  t.year,
  t.month,
  t.day,
  t."$path" AS src,
  t.line
FROM glue-database-catalog.etl_batch_runner_log_raw AS t;



-- define reporting
WITH extract_logs AS (
  SELECT
    r.job,
    r.year,
    r.month,
    r.day,
    format('%04d-%02d-%02d', r.year, r.month, r.day)                                     AS dt,
    regexp_extract(r.src, '/logs/cron-batch/([^/]+)/', 1)                                AS source_name,
    COALESCE(
      MAX(regexp_extract(r.line, '"pg_table"\s*:\s*"([^"]+)"', 1)),
      MAX(regexp_extract(r.line, '"prod_table"\s*:\s*"([^"]+)"', 1)),
      MAX(regexp_extract(r.line, '"staging_table"\s*:\s*"([^"]+)"', 1))
    )                                                                                    AS table_name,

    -- fields result extarct 1x per file
    MAX(regexp_extract(r.line, '"run_id"\s*:\s*"([^"]+)"', 1))                           AS run_id,
    MAX(regexp_extract(r.line, '"job"\s*:\s*"([^"]+)"', 1))                              AS job_name_in_file,
    CAST(MAX(regexp_extract(r.line, '"exit_code"\s*:\s*([0-9]+)', 1)) AS integer)        AS exit_code,
    MAX(regexp_extract(r.line, '"status"\s*:\s*"([^"]+)"', 1))                           AS status,
    MAX(regexp_extract(r.line, '"host"\s*:\s*"([^"]+)"', 1))                             AS host,
    MAX(regexp_extract(r.line, '"pg_schema"\s*:\s*"([^"]+)"', 1))                        AS pg_schema,
    MAX(regexp_extract(r.line, '"staging_table"\s*:\s*"([^"]+)"', 1))                    AS staging_table,
    MAX(regexp_extract(r.line, '"prod_table"\s*:\s*"([^"]+)"', 1))                       AS prod_table,
    MAX(regexp_extract(r.line, '"started_at_utc"\s*:\s*"([^"]+)"', 1))                   AS started_at_utc,
    MAX(regexp_extract(r.line, '"ended_at_utc"\s*:\s*"([^"]+)"', 1))                     AS ended_at_utc,
    MAX(regexp_extract(r.line, '"started_at_wib"\s*:\s*"([^"]+)"', 1))                   AS started_at_wib,
    MAX(regexp_extract(r.line, '"ended_at_wib"\s*:\s*"([^"]+)"', 1))                     AS ended_at_wib,
    MAX(regexp_extract(r.line, '"timezone_wib"\s*:\s*"([^"]+)"', 1))                     AS timezone_wib,
    CAST(MAX(regexp_extract(r.line, '"duration_seconds"\s*:\s*([0-9]+)', 1)) AS integer) AS duration_seconds,
    MAX(regexp_extract(r.line, '"log_s3_uri"\s*:\s*"([^"]+)"', 1))                       AS log_s3_uri,
    r.src                                                                                AS src
  FROM glue-database-catalog.view_etl_batch_runner_raw AS r
  WHERE
    system = 'database-name'
    AND r.job IN ('table-name', 'table-name')
    AND r.year  = 2025
    AND r.month = 9
    AND r.day BETWEEN 17 AND 21 -- replace with range date
    AND r.src LIKE '%/monitoring/status.json'
  GROUP BY r.job, r.year, r.month, r.day, r.src
)

SELECT
  dt,
  source_name AS source,
  table_name,
  COUNT(*) AS total_jobs,
  COUNT_IF( (lower(status) IN ('success','ok')) AND exit_code = 0 ) AS success_count,
  COUNT_IF( NOT ((lower(status) IN ('success','ok')) AND exit_code = 0) ) AS error_count,
  100 - ROUND( (COUNT_IF( (lower(status) IN ('success','ok')) AND exit_code = 0 ) * 100.0 / COUNT(*)), 2 ) AS error_rate_pct,
  ROUND( (COUNT_IF( (lower(status) IN ('success','ok')) AND exit_code = 0 ) * 100.0 / COUNT(*)), 2 ) AS success_rate_pct
FROM extract_logs
WHERE dt BETWEEN '2025-09-17' AND '2025-09-21'
GROUP BY dt, source_name, table_name
ORDER BY table_name
