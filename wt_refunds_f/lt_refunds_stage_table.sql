CREATE EXTERNAL TABLE `hmdb_v3_transform.lt_refunds_stage`(
  `client_abbr` string, 
  `accnt_nbr` bigint, 
  `transaction_nbr` decimal(18,0), 
  `operator_id` int, 
  `transaction_type_cd` string, 
  `product_line_abbr` string, 
  `order_nbr` bigint, 
  `refund_amt` decimal(7,2), 
  `refund_method_cd` string, 
  `refund_reason_cd` string, 
  `refund_src_cd` string, 
  `refund_dt` date, 
  `reported_undeliverable_cd` string, 
  `reaplied_ind` string, 
  `mgrt_product_line_abbr` string, 
  `refund_status_cd` string, 
  `update_dt` timestamp)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://cds-core-dev/hmdb/ETL/cooked/lt_refunds_stage/'
