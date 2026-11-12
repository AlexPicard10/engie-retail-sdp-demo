-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Gold · Customer energy profile — one row per customer
-- MAGIC #### A **Materialized View**: a join + aggregation, stored and refreshed on each pipeline update.
-- MAGIC This is the analytical backbone: one row per customer combining **consumption habits**
-- MAGIC (from `silver_consumption`), **billing behaviour** (from `silver_billing`) and the
-- MAGIC **current tariff** (carried on `silver_customers`). Because it is read often and costs a
-- MAGIC join + two aggregations, we materialize it — readers get a precomputed result.
-- MAGIC
-- MAGIC Six features are derived:
-- MAGIC `avg_daily_kwh`, `peak_consumption_pct`, `weekend_ratio`, `seasonal_ratio`,
-- MAGIC `consumption_trend`, `payment_reliability_pct`. From them we add a **rule-based
-- MAGIC `consumption_segment`** — a transparent heuristic (the heavier sibling demo learns the same
-- MAGIC three segments with K-Means). `NULLIF` guards every ratio so nobody divides by zero.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW gold_customer_energy_profile
CLUSTER BY AUTO
COMMENT "One row per customer: 6 consumption/billing features + tariff + rule-based segment — gold layer"
TBLPROPERTIES ('quality' = 'gold')
AS
-- Step 1 (CTE): consumption habits per customer
WITH consumption_feats AS (
  SELECT
    customer_id,
    ROUND(AVG(kwh_consumed), 3)                                                       AS avg_daily_kwh,
    ROUND(  SUM(CASE WHEN is_weekend      THEN kwh_consumed ELSE 0 END)
          / NULLIF(SUM(CASE WHEN NOT is_weekend THEN kwh_consumed ELSE 0 END), 0), 3) AS weekend_ratio,
    ROUND(  SUM(CASE WHEN season = 'winter' THEN kwh_consumed ELSE 0 END)
          / NULLIF(SUM(CASE WHEN season = 'summer' THEN kwh_consumed ELSE 0 END), 0), 3) AS seasonal_ratio,
    -- Simple deterministic trend: 2nd-half vs 1st-half average daily kWh.
    ROUND(  AVG(CASE WHEN reading_month >= 7 THEN kwh_consumed END)
          - AVG(CASE WHEN reading_month <= 6 THEN kwh_consumed END), 3)               AS consumption_trend
  FROM silver_consumption
  GROUP BY customer_id
),
-- Step 2 (CTE): billing behaviour per customer
billing_feats AS (
  SELECT
    customer_id,
    ROUND(100.0 * SUM(peak_kwh) / NULLIF(SUM(kwh_billed), 0), 2) AS peak_consumption_pct,
    ROUND(100.0 * AVG(CASE WHEN paid_flag THEN 1 ELSE 0 END), 2) AS payment_reliability_pct
  FROM silver_billing
  GROUP BY customer_id
)
-- Step 3: assemble the profile and label a segment
SELECT
  c.customer_id,
  c.first_name,
  c.last_name,
  c.region,
  c.customer_type,
  cf.avg_daily_kwh,
  bf.peak_consumption_pct,
  cf.weekend_ratio,
  cf.seasonal_ratio,
  cf.consumption_trend,
  bf.payment_reliability_pct,
  c.plan_id,
  c.plan_name,
  c.plan_type,
  c.peak_rate_eur_kwh,
  c.off_peak_rate_eur_kwh,
  c.is_flat_tariff,
  -- Rule-based segment. Peak Heavy = high peak share; among the rest, a strong winter/summer
  -- swing marks a Seasonal Spiker, otherwise a (low, reliable) Green Saver.
  CASE
    WHEN bf.peak_consumption_pct >= 72 THEN 'Peak Heavy'
    WHEN cf.seasonal_ratio       >= 2.5 THEN 'Seasonal Spiker'
    ELSE 'Green Saver'
  END AS consumption_segment
FROM silver_customers c
LEFT JOIN consumption_feats cf ON c.customer_id = cf.customer_id
LEFT JOIN billing_feats    bf  ON c.customer_id = bf.customer_id;
