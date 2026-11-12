-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Bronze · Customer interactions — Auto Loader on nested JSON
-- MAGIC #### 30k support interactions, newline-delimited JSON with 3 levels of nesting.
-- MAGIC Auto Loader reads JSON exactly like CSV — only `format => 'json'` changes. With
-- MAGIC `inferColumnTypes => true` the nested objects (`interaction`, `sentiment`, `details`) land as
-- MAGIC **`struct` columns**; we flatten the fields we need in the silver layer
-- MAGIC (`DLT-Interactions-Silver.sql`). `raw_message` sits at the root as a plain string.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bronze_interactions
CLUSTER BY AUTO
COMMENT "Raw customer interactions (nested JSON: interaction / sentiment / details) — bronze layer"
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  *,
  _metadata.file_path              AS _source_file,
  _metadata.file_modification_time AS _source_file_ts,
  current_timestamp()              AS _ingested_at
FROM STREAM read_files(
  '${interactions_path}',
  format            => 'json',
  inferColumnTypes  => true,
  schemaEvolutionMode => 'rescue'
);
