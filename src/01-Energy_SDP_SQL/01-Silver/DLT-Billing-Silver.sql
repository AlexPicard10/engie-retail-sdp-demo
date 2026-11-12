-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Silver · Billing — typing & quality
-- MAGIC #### Streaming Table + **Expectations** on the monthly bills.
-- MAGIC Types the money and kWh columns and enforces basic sanity: a known payment status, a
-- MAGIC non-negative amount, and a peak/off-peak split that reconciles with the billed total
-- MAGIC (allowing a small rounding tolerance). The `paid_flag` derived here makes the gold
-- MAGIC `payment_reliability_pct` a plain average.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE silver_billing
(
  CONSTRAINT valid_customer_id     EXPECT (customer_id IS NOT NULL)                                          ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_payment_status  EXPECT (payment_status IN ('paid','pending','overdue','partial'))         ON VIOLATION DROP ROW,
  CONSTRAINT non_negative_amount   EXPECT (amount_eur >= 0)                                                  ON VIOLATION DROP ROW,
  CONSTRAINT split_reconciles      EXPECT (abs((peak_kwh + off_peak_kwh) - kwh_billed) <= 1.0)
)
CLUSTER BY AUTO
COMMENT "Monthly bills, typed, with a paid_flag for payment reliability — silver layer"
TBLPROPERTIES ('quality' = 'silver')
AS
SELECT
  bill_id,
  customer_id,
  billing_period,
  to_date(billing_start_date)        AS billing_start_date,
  to_date(billing_end_date)          AS billing_end_date,
  CAST(kwh_billed   AS DOUBLE)       AS kwh_billed,
  CAST(peak_kwh     AS DOUBLE)       AS peak_kwh,
  CAST(off_peak_kwh AS DOUBLE)       AS off_peak_kwh,
  CAST(amount_ht_eur AS DOUBLE)      AS amount_ht_eur,
  CAST(tax_eur       AS DOUBLE)      AS tax_eur,
  CAST(amount_eur    AS DOUBLE)      AS amount_eur,
  payment_status,
  (payment_status = 'paid')          AS paid_flag,
  to_date(due_date)                  AS due_date,
  to_date(payment_date)              AS payment_date
FROM STREAM(bronze_billing);
