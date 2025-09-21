CREATE TABLE database-glue-catalog.tablename (
  column_name data_type, 
  __op string,
  __ts_ms timestamp)
LOCATION 's3://bucket-name/silver-layer/databasename/tablename/'
TBLPROPERTIES (
  'table_type'='iceberg',
  'write_compression'='ZSTD',
  'compression_level'='3',
  'optimize_rewrite_delete_file_threshold'='2',
  'optimize_rewrite_data_file_threshold'='5',
  'vacuum_min_snapshots_to_keep'='1',
  'vacuum_max_snapshot_age_seconds'='3600',
  'format'='PARQUET'
);
