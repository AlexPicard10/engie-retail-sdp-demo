-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Gold · Customer topics — a light AI touch with `ai_classify`
-- MAGIC #### A **Materialized View** that labels each customer's latest message with a built-in AI function.
-- MAGIC `ai_classify(text, ARRAY(labels...))` is a **SQL AI function** backed by a Databricks
-- MAGIC Foundation Model endpoint — no model to train, deploy or manage. It returns the best-fitting
-- MAGIC label from the list we give it.
-- MAGIC
-- MAGIC **Classify once, fan out.** There are only ~80 *distinct* messages across 30,000 interactions
-- MAGIC (short, templated support texts). We run `ai_classify` over the **`DISTINCT` messages** and
-- MAGIC join the result back to each customer's **most recent** interaction (`ROW_NUMBER` … `= 1`) —
-- MAGIC roughly **350× fewer LLM calls** than classifying every row, same answer.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW gold_customer_topics
CLUSTER BY AUTO
COMMENT "Each customer's most recent interaction, with an ai_classify topic label — gold layer"
TBLPROPERTIES ('quality' = 'gold')
AS
-- Step 1 (CTE): the most recent interaction per customer
WITH latest_interaction AS (
  SELECT customer_id, raw_message, interaction_ts
  FROM (
    SELECT
      customer_id,
      raw_message,
      interaction_ts,
      ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY interaction_ts DESC) AS rn
    FROM silver_interactions
  )
  WHERE rn = 1
),
-- Step 2 (CTE): the ~80 distinct messages
distinct_msgs AS (
  SELECT DISTINCT raw_message
  FROM silver_interactions
),
-- Step 3 (CTE): classify each distinct message once
classified AS (
  SELECT
    raw_message,
    ai_classify(
      raw_message,
      ARRAY('billing dispute', 'service outage', 'tariff inquiry', 'meter issue', 'general feedback')
    ) AS topic
  FROM distinct_msgs
)
-- Step 4: fan the topic back out to each customer's latest message
SELECT
  li.customer_id,
  li.raw_message,
  li.interaction_ts AS last_interaction_ts,
  cl.topic
FROM latest_interaction li
LEFT JOIN classified cl
  ON li.raw_message = cl.raw_message;
