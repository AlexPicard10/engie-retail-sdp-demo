-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Silver · Interactions — flattening the nested JSON
-- MAGIC #### Streaming Table + **Expectations**, pulling the nested `struct` fields up to columns.
-- MAGIC Auto Loader inferred the JSON objects as `struct` columns (`interaction`, `sentiment`,
-- MAGIC `details`). Here we reach into them with **dot notation** (`interaction.category`,
-- MAGIC `sentiment.score`, `details.resolution.status`) and flatten them into a clean, typed row —
-- MAGIC no UDFs, no explode, just SQL field access.
-- MAGIC
-- MAGIC `interaction.category` already carries the business reason for each contact (e.g.
-- MAGIC `billing_dispute`); we keep it alongside `raw_message`, which the gold layer runs through
-- MAGIC `ai_classify` for a friendly, consumer-facing `topic` label.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE silver_interactions
(
  CONSTRAINT valid_customer_id    EXPECT (customer_id IS NOT NULL)            ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_interaction_id EXPECT (interaction_id IS NOT NULL)         ON VIOLATION DROP ROW,
  CONSTRAINT valid_sentiment      EXPECT (sentiment_score BETWEEN 1 AND 5),
  CONSTRAINT known_category       EXPECT (category IS NOT NULL)
)
CLUSTER BY AUTO
COMMENT "Customer interactions flattened from nested JSON (type, category, sentiment, resolution) — silver layer"
TBLPROPERTIES ('quality' = 'silver')
AS
SELECT
  interaction_id,
  customer_id,
  to_timestamp(interaction_date)                 AS interaction_ts,
  channel,
  raw_message,
  interaction.type                               AS interaction_type,
  interaction.priority                           AS priority,
  interaction.category                           AS category,
  CAST(sentiment.score AS DOUBLE)                AS sentiment_score,
  sentiment.label                                AS sentiment_label,
  details.resolution.status                      AS resolution_status,
  details.resolution.action                      AS resolution_action
FROM STREAM(bronze_interactions);
