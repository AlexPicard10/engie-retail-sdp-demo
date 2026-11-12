-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Silver · Consumption — typing, quality & calendar features
-- MAGIC #### Streaming Table + **Expectations**, with derived calendar columns for the gold features.
-- MAGIC We cast the reading timestamp to a date and derive `month`, `is_weekend` and `season`
-- MAGIC (winter = Nov–Feb, summer = Jun–Aug). These feed the gold `weekend_ratio` and
-- MAGIC `seasonal_ratio` features with simple `SUM(CASE …)` arithmetic — no need to re-parse dates
-- MAGIC downstream.
-- MAGIC
-- MAGIC The generator injects ~5% anomalies (consumption spikes and near-zero flatlines). The
-- MAGIC `positive_consumption` expectation drops the physically-impossible readings while keeping the
-- MAGIC small-but-valid flatline values, so the anomaly signal survives into analytics.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE silver_consumption
(
  CONSTRAINT valid_customer_id     EXPECT (customer_id IS NOT NULL)   ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_reading_date    EXPECT (reading_date IS NOT NULL)  ON VIOLATION DROP ROW,
  CONSTRAINT positive_consumption  EXPECT (kwh_consumed >= 0)         ON VIOLATION DROP ROW
)
CLUSTER BY AUTO
COMMENT "Daily meter readings, typed, with month / is_weekend / season calendar features — silver layer"
TBLPROPERTIES ('quality' = 'silver')
AS
-- `timestamp` is the raw column name (and a SQL keyword), so it is backticked.
SELECT
  reading_id,
  customer_id,
  to_date(`timestamp`)                       AS reading_date,
  CAST(kwh_consumed AS DOUBLE)               AS kwh_consumed,
  meter_type,
  month(to_date(`timestamp`))                AS reading_month,
  -- Spark dayofweek(): 1 = Sunday … 7 = Saturday.
  (dayofweek(to_date(`timestamp`)) IN (1, 7)) AS is_weekend,
  CASE
    WHEN month(to_date(`timestamp`)) IN (11, 12, 1, 2) THEN 'winter'
    WHEN month(to_date(`timestamp`)) IN (6, 7, 8)      THEN 'summer'
    ELSE 'shoulder'
  END                                         AS season
FROM STREAM(bronze_consumption);
