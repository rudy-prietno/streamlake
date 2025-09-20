CREATE TABLE monitoring.data_quality_check (
  id string,
  database_source string,
  catalog_name string,
  table_name string,
  created_at date,
  created_hours string,
  total_row_silver bigint,
  total_row_source bigint,
  status string,
  diff bigint,
  large string,
  ingestion_at timestamp
  )
LOCATION 's3://s3-bucket/data-quality/monitoring/dql/'
TBLPROPERTIES (
  'table_type'='iceberg',
  'vacuum_max_snapshot_age_seconds'='3600',
  'format'='PARQUET',
  'write_compression'='zstd',
  'compression_level'='3',
  'optimize_rewrite_delete_file_threshold'='2',
  'optimize_rewrite_data_file_threshold'='5',
  'vacuum_min_snapshots_to_keep'='1'
);





CREATE EXTERNAL TABLE IF NOT EXISTS monitoring.dq_runner_log (
  DATABASE_SOURCE        string,
  TABLE_NAME             string,
  RUN_START_ISO          string,
  RUN_END_ISO            string,
  JOB_STATUS             string,
  TOTAL_DURATIONS        int
)
PARTITIONED BY (dt string, hour string, source string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar'     = '"',
  'escapeChar'    = '\\'
)
LOCATION 's3://s3-bucket/logs/data-quality-logs/dql/'
TBLPROPERTIES ('skip.header.line.count'='1');

MSCK REPAIR TABLE monitoring.dq_runner_log;

SELECT
  dt,
  source,
  table_name,
  COUNT(*) AS total_jobs,
  COUNT_IF(JOB_STATUS = 'OK') AS success_count,
  COUNT_IF(JOB_STATUS = 'ERR') AS error_count,
  100 - ROUND(
    (COUNT_IF(JOB_STATUS = 'OK') * 100.0 / COUNT(*)),
    2
  ) AS error_rate_pct,
  ROUND(
    (COUNT_IF(JOB_STATUS = 'OK') * 100.0 / COUNT(*)),
    2
  ) AS success_rate_pct
FROM monitoring.dq_runner_log
WHERE dt = '2025-09-19' 
  AND source like 'db_source'
GROUP BY dt, source, table_name;
