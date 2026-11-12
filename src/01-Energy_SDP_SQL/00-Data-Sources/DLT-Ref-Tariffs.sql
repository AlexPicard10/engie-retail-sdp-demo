-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Bronze · Tariff plans — static reference as a Materialized View
-- MAGIC #### 11 tariff plans: small, static reference data → a **Materialized View**, not a stream.
-- MAGIC A reference table of 11 rows has no reason to stream. We read it as a **batch**
-- MAGIC `read_files(...)` (no `STREAM`) inside a **Materialized View**: the result is stored and
-- MAGIC recomputed on each pipeline update. This is the right shape for small, slowly-changing
-- MAGIC lookups — and it contrasts nicely with the streaming bronze tables.
-- MAGIC
-- MAGIC We also derive **`is_flat_tariff`** here: a plan is "flat" when its peak and off-peak rates are
-- MAGIC (nearly) identical — these customers pay the peak rate around the clock, with no off-peak
-- MAGIC relief. That flag is the heart of the retention story downstream.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW ref_tariff_plans
CLUSTER BY AUTO
COMMENT "Tariff plan reference (11 plans) with a derived is_flat_tariff flag — bronze reference"
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  plan_id,
  name AS plan_name,
  CAST(peak_rate_eur_kwh      AS DOUBLE) AS peak_rate_eur_kwh,
  CAST(off_peak_rate_eur_kwh  AS DOUBLE) AS off_peak_rate_eur_kwh,
  CAST(standing_charge_eur_month AS DOUBLE) AS standing_charge_eur_month,
  CAST(green_energy_pct       AS INT)    AS green_energy_pct,
  plan_type,
  -- Flat tariff: peak and off-peak rates differ by less than 2 c€/kWh.
  (abs(CAST(peak_rate_eur_kwh AS DOUBLE) - CAST(off_peak_rate_eur_kwh AS DOUBLE)) < 0.02) AS is_flat_tariff
FROM read_files(
  '${tariffs_path}',
  format            => 'csv',
  header            => true,
  inferColumnTypes  => true
);
