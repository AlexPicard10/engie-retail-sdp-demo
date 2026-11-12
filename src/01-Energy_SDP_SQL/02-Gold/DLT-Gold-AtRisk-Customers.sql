-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Gold · At-risk retention list — the payoff
-- MAGIC #### A **Materialized View** that joins the two gold tables into one ranked call list.
-- MAGIC The business question the whole pipeline exists to answer: **which high-value customers are
-- MAGIC most exposed to churn?** The answer sits at the intersection of three signals no single raw
-- MAGIC file holds:
-- MAGIC
-- MAGIC 1. **Peak Heavy** — high peak-hour consumption (`consumption_segment`), the highest-revenue customers;
-- MAGIC 2. **Flat tariff** — `is_flat_tariff`, so they pay the peak rate around the clock with no off-peak relief;
-- MAGIC 3. **Billing dispute** — their most recent interaction `topic` (from `ai_classify`) is a billing complaint.
-- MAGIC
-- MAGIC Ranked by estimated annual revenue at risk, this is the list a retention team can act on today.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW gold_at_risk_customers
CLUSTER BY AUTO
COMMENT "Peak Heavy + flat-tariff + billing-dispute customers, ranked by revenue at risk — gold layer"
TBLPROPERTIES ('quality' = 'gold')
AS
SELECT
  p.customer_id,
  p.first_name,
  p.last_name,
  p.region,
  p.plan_name,
  p.consumption_segment,
  p.avg_daily_kwh,
  p.peak_consumption_pct,
  p.payment_reliability_pct,
  t.topic,
  -- Rough annual revenue at risk: avg daily kWh × 365 × peak rate
  -- (on a flat tariff the peak rate applies all day).
  ROUND(p.avg_daily_kwh * 365 * p.peak_rate_eur_kwh, 0) AS est_annual_revenue_eur
FROM gold_customer_energy_profile p
INNER JOIN gold_customer_topics t
  ON p.customer_id = t.customer_id
WHERE p.consumption_segment = 'Peak Heavy'
  AND p.is_flat_tariff = true
  AND t.topic = 'billing dispute'
ORDER BY est_annual_revenue_eur DESC;
