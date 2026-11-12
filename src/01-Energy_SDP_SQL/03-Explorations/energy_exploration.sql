-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Explorations — validate the pipeline output
-- MAGIC These queries are **not** part of the pipeline. Run them on a **SQL Warehouse** (or any
-- MAGIC notebook) after the pipeline completes, against the output schema
-- MAGIC `${catalog}.engie_retail_en_sql`. Replace `alp_demo_catalog` below with your catalog, or
-- MAGIC run `USE CATALOG <your_catalog>; USE SCHEMA engie_retail_en_sql;` first.

-- COMMAND ----------

USE CATALOG alp_demo_catalog;
USE SCHEMA engie_retail_en_sql;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1 / Row counts per layer (bronze ≈ raw, gold = 1 row per customer)

-- COMMAND ----------

SELECT 'bronze_customers'    AS table, COUNT(*) AS rows FROM bronze_customers
UNION ALL SELECT 'bronze_consumption',  COUNT(*) FROM bronze_consumption
UNION ALL SELECT 'bronze_billing',      COUNT(*) FROM bronze_billing
UNION ALL SELECT 'bronze_interactions', COUNT(*) FROM bronze_interactions
UNION ALL SELECT 'silver_customers',    COUNT(*) FROM silver_customers
UNION ALL SELECT 'silver_consumption',  COUNT(*) FROM silver_consumption
UNION ALL SELECT 'silver_billing',      COUNT(*) FROM silver_billing
UNION ALL SELECT 'silver_interactions', COUNT(*) FROM silver_interactions
UNION ALL SELECT 'gold_customer_energy_profile', COUNT(*) FROM gold_customer_energy_profile
UNION ALL SELECT 'gold_customer_topics',         COUNT(*) FROM gold_customer_topics
UNION ALL SELECT 'gold_at_risk_customers',       COUNT(*) FROM gold_at_risk_customers
ORDER BY table;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2 / Segment distribution + how many sit on a flat tariff

-- COMMAND ----------

SELECT
  consumption_segment,
  COUNT(*)                                        AS customers,
  ROUND(AVG(avg_daily_kwh), 1)                    AS avg_daily_kwh,
  ROUND(AVG(peak_consumption_pct), 1)             AS avg_peak_pct,
  SUM(CASE WHEN is_flat_tariff THEN 1 ELSE 0 END) AS on_flat_tariff
FROM gold_customer_energy_profile
GROUP BY consumption_segment
ORDER BY customers DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3 / Topic mix from `ai_classify`

-- COMMAND ----------

SELECT topic, COUNT(*) AS customers
FROM gold_customer_topics
GROUP BY topic
ORDER BY customers DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4 / The at-risk retention list + total revenue at risk

-- COMMAND ----------

SELECT COUNT(*) AS at_risk_customers,
       ROUND(SUM(est_annual_revenue_eur), 0) AS total_revenue_at_risk_eur
FROM gold_at_risk_customers;

-- COMMAND ----------

SELECT *
FROM gold_at_risk_customers
ORDER BY est_annual_revenue_eur DESC
LIMIT 50;
