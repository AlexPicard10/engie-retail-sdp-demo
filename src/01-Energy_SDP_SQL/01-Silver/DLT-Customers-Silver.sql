-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Silver · Customers — quality, typing & tariff enrichment
-- MAGIC #### Streaming Table + **Expectations**, with a **stream-static join** to the tariff reference.
-- MAGIC **Expectations** (`CONSTRAINT … EXPECT`) declare quality rules right in the pipeline:
-- MAGIC a missing key fails the update (`FAIL UPDATE`), out-of-range rows are dropped (`DROP ROW`),
-- MAGIC softer rules just warn (tracked in the pipeline's data-quality metrics).
-- MAGIC
-- MAGIC We type the raw columns in a `typed` CTE, then **LEFT JOIN the `ref_tariff_plans`
-- MAGIC Materialized View** to attach the plan details and the `is_flat_tariff` flag. Joining a
-- MAGIC streaming source to a static table is a **stream-static join** — each customer row is
-- MAGIC enriched with its tariff as it flows through.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE silver_customers
(
  CONSTRAINT valid_customer_id    EXPECT (customer_id IS NOT NULL)                       ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_customer_type  EXPECT (customer_type IN ('residential','commercial')) ON VIOLATION DROP ROW,
  CONSTRAINT valid_churn_score    EXPECT (churn_risk_score BETWEEN 0 AND 1),
  CONSTRAINT known_tariff_plan    EXPECT (plan_id IS NOT NULL)
)
CLUSTER BY AUTO
COMMENT "Customers cleaned, typed and enriched with their tariff plan (incl. is_flat_tariff) — silver layer"
TBLPROPERTIES ('quality' = 'silver')
AS
-- Step 1 (CTE): type the raw customer columns
WITH typed AS (
  SELECT
    customer_id,
    initcap(first_name)                 AS first_name,
    upper(last_name)                    AS last_name,
    lower(email)                        AS email,
    customer_type,
    contract_type,
    tariff_plan_id,
    CAST(subscribed_power_kva AS INT)   AS subscribed_power_kva,
    meter_type,
    region,
    city,
    to_date(signup_date)                AS signup_date,
    dwelling_type,
    heating_type,
    CAST(household_size AS INT)         AS household_size,
    CAST(has_ev_charger AS BOOLEAN)     AS has_ev_charger,
    CAST(has_solar_panels AS BOOLEAN)   AS has_solar_panels,
    CAST(anomaly_flag AS BOOLEAN)       AS anomaly_flag,
    anomaly_type,
    CAST(churn_risk_score AS DOUBLE)    AS churn_risk_score
  FROM STREAM(bronze_customers)
)
-- Step 2: enrich with the tariff plan (stream-static join to the reference MV)
SELECT
  t.*,
  p.plan_id,
  p.plan_name,
  p.plan_type,
  p.peak_rate_eur_kwh,
  p.off_peak_rate_eur_kwh,
  p.green_energy_pct,
  p.is_flat_tariff
FROM typed t
LEFT JOIN ref_tariff_plans p
  ON t.tariff_plan_id = p.plan_id;
