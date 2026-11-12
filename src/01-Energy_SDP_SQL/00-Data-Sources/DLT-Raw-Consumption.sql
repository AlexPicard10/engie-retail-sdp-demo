-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Bronze · Daily smart-meter readings — Auto Loader
-- MAGIC #### ~3.65M daily readings (10k customers × 365 days), one flat CSV.
-- MAGIC Same Auto Loader pattern as the customer master. This is the largest source; Auto Loader's
-- MAGIC incremental file tracking means a re-run after new files are dropped only reads the new
-- MAGIC files, not the whole history.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bronze_consumption
CLUSTER BY AUTO
COMMENT "Raw daily smart-meter readings (reading_id, customer_id, timestamp, kwh_consumed) — bronze layer"
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  *,
  _metadata.file_path              AS _source_file,
  _metadata.file_modification_time AS _source_file_ts,
  current_timestamp()              AS _ingested_at
FROM STREAM read_files(
  '${consumption_path}',
  format            => 'csv',
  header            => true,
  inferColumnTypes  => true,
  schemaEvolutionMode => 'rescue'
);
