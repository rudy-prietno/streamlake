CREATE EXTERNAL TABLE `database-glue-catalog.tablename`(
  column_name data_type, 
  `__op` string, 
  `__ts_ms` timestamp)
PARTITIONED BY ( 
  `yyyy` int, 
  `mm` int, 
  `dd` int, 
  `hh` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://bucket-name/stream-stg/databasename/tablename'
TBLPROPERTIES (
  'projection.dd.digits'='2', 
  'projection.dd.range'='1,31', 
  'projection.dd.type'='integer', 
  'projection.enabled'='true', 
  'projection.hh.digits'='2', 
  'projection.hh.range'='0,23', 
  'projection.hh.type'='integer', 
  'projection.mm.digits'='2', 
  'projection.mm.range'='1,12', 
  'projection.mm.type'='integer', 
  'projection.yyyy.range'='2020,2035', 
  'projection.yyyy.type'='integer', 
  'storage.location.template'='s3://bucket-name/stream-stg/databasename/tablename/${yyyy}/${mm}/${dd}/${hh}/', 
  'transient_lastDdlTime'='1757856781')
