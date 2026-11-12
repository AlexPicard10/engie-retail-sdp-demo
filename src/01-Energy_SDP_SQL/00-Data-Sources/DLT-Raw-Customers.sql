-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Retail Energy 360 — Lakeflow Declarative Pipeline (SDP / LDP), SQL
-- MAGIC ### From 5 raw smart-meter files to a named, quantified retention list
-- MAGIC
-- MAGIC A deliberately **simple** declarative pipeline on synthetic French retail-energy data
-- MAGIC (fictional utility). Five raw files land in a Unity Catalog Volume, are ingested with
-- MAGIC **Auto Loader**, cleaned with **Expectations**, and aggregated into gold **Materialized
-- MAGIC Views** — ending on the at-risk call list: the high-consumption **Peak Heavy** customers
-- MAGIC stuck on a **flat tariff** who are already **disputing their bills**.
-- MAGIC
-- MAGIC | Step | Lakeflow (SDP) feature |
-- MAGIC |---|---|
-- MAGIC | File ingestion (UC Volume) | **Streaming Table** + **Auto Loader** (`STREAM read_files`) — *bronze* |
-- MAGIC | Static reference data (11 tariff plans) | **Materialized View** (batch `read_files`) — *bronze* |
-- MAGIC | Cleaning, typing, derived columns | **Streaming Table** + **Expectations** — *silver* |
-- MAGIC | Attaching the tariff | **stream-static join** to the tariff reference — *silver* |
-- MAGIC | Nested JSON flattening (interactions) | `struct` field access on an Auto Loader JSON source — *silver* |
-- MAGIC | Per-customer features + rule-based segment | **Materialized View** (aggregation + join) — *gold* |
-- MAGIC | Interaction topic | **`ai_classify`** AI function over ~80 distinct messages — *gold* |
-- MAGIC | At-risk retention list | **Materialized View** (join + filter) — *gold* |
-- MAGIC | Output / consumption | Governed by **Unity Catalog** (AI/BI, Genie, apps) |
-- MAGIC
-- MAGIC Every persisted table uses `CLUSTER BY AUTO` (automatic liquid clustering — Databricks picks
-- MAGIC and evolves the clustering keys from the queries actually run, no manual partitioning).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Bronze · Customer master — ingested with Auto Loader (`STREAM read_files`)
-- MAGIC #### `STREAM read_files(...)` detects and ingests new files dropped into a Volume folder.
-- MAGIC Auto Loader handles schema inference, schema evolution (`rescue` mode) and incremental file
-- MAGIC tracking: the first run loads the history, subsequent runs only ingest newly dropped files
-- MAGIC (fast, cheap incremental processing).
-- MAGIC
-- MAGIC **One source per file ⇒ one streaming query.** Unlike a multi-source fan-in (which would use
-- MAGIC Append Flows), each raw file has a single landing folder, so a plain
-- MAGIC `CREATE OR REFRESH STREAMING TABLE … AS SELECT … FROM STREAM read_files(...)` is all we need.
-- MAGIC We keep the Auto Loader technical columns (`_metadata.*`, `_ingested_at`) for lineage.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bronze_customers
CLUSTER BY AUTO
COMMENT "Raw customer master (10k customers) ingested from the landing/customers Volume folder — bronze layer"
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  *,
  _metadata.file_path              AS _source_file,
  _metadata.file_modification_time AS _source_file_ts,
  current_timestamp()              AS _ingested_at
FROM STREAM read_files(
  '${customers_path}',
  format            => 'csv',
  header            => true,
  inferColumnTypes  => true,
  schemaEvolutionMode => 'rescue'
);
