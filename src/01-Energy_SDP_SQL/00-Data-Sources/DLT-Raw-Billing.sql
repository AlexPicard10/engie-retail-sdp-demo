-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Bronze · Monthly bills — Auto Loader
-- MAGIC #### 120k monthly bills (10k customers × 12 months).
-- MAGIC Carries the peak / off-peak kWh split and the payment status — the raw material for the
-- MAGIC gold `peak_consumption_pct` and `payment_reliability_pct` features.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bronze_billing
CLUSTER BY AUTO
COMMENT "Raw monthly bills (kwh_billed, peak_kwh, off_peak_kwh, amount_eur, payment_status) — bronze layer"
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  *,
  _metadata.file_path              AS _source_file,
  _metadata.file_modification_time AS _source_file_ts,
  current_timestamp()              AS _ingested_at
FROM STREAM read_files(
  '${billing_path}',
  format            => 'csv',
  header            => true,
  inferColumnTypes  => true,
  schemaEvolutionMode => 'rescue'
);
