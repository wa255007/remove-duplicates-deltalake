CREATE EXTERNAL TABLE `orders_duplicate`(
 order_id integer, 
  user_id integer, 
  order_date string, 
  status string,  
  `load_time` timestamp)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://***your s3 path here***/glue-remove-dups_output/_symlink_format_manifest'
TBLPROPERTIES (
  'transient_lastDdlTime'='1652949883')
  
