# SDP pipeline — Retail Energy 360 (SQL)

Pipeline `Energy_Retail_SQL_<target>` → schema `alp_demo_catalog.engie_retail_en_sql`. Queries are
written with **CTEs** (`WITH … AS`) rather than subqueries: one step = one name, read top to bottom.

Graph order (the pipeline resolves dependencies automatically — file order does not matter):

```
00-Data-Sources/
  DLT-Raw-Customers.sql      -> bronze_customers      (Streaming Table + Auto Loader, CSV)
  DLT-Raw-Consumption.sql    -> bronze_consumption    (Streaming Table + Auto Loader, CSV)
  DLT-Raw-Billing.sql        -> bronze_billing        (Streaming Table + Auto Loader, CSV)
  DLT-Raw-Interactions.sql   -> bronze_interactions   (Streaming Table + Auto Loader, nested JSON)
  DLT-Ref-Tariffs.sql        -> ref_tariff_plans      (Materialized View, batch read_files + is_flat_tariff)

01-Silver/
  DLT-Customers-Silver.sql     -> silver_customers      (Streaming Table + Expectations + stream-static tariff join)
  DLT-Consumption-Silver.sql   -> silver_consumption    (Streaming Table + Expectations + month/is_weekend/season)
  DLT-Billing-Silver.sql       -> silver_billing        (Streaming Table + Expectations + paid_flag)
  DLT-Interactions-Silver.sql  -> silver_interactions   (Streaming Table + Expectations + nested-JSON flatten)

02-Gold/
  DLT-Gold-Customer-Energy-Profile.sql -> gold_customer_energy_profile (Materialized View: 6 features + segment)
  DLT-Gold-Customer-Topics.sql         -> gold_customer_topics         (Materialized View: DISTINCT -> ai_classify -> join back)
  DLT-Gold-AtRisk-Customers.sql        -> gold_at_risk_customers       (Materialized View: the retention list)
```

Every persisted table uses `CLUSTER BY AUTO` (automatic liquid clustering).

`03-Explorations/` is **not** part of the pipeline (validation queries to run on a SQL Warehouse).

SDP features showcased: **Streaming Table**, **Auto Loader** (`STREAM read_files`, CSV + JSON),
**Materialized View** (static reference + gold aggregations), **Expectations**, **stream-static
join**, **nested-JSON flattening**, **`ai_classify`** AI function, **CLUSTER BY AUTO**.

Deliberately *not* here (kept simple): Append Flows, Auto CDC / SCD, window-function dedup, ML.
