# Retail Energy 360 — Spark Declarative Pipelines (SQL)

A deliberately **simple** Lakeflow **Spark Declarative Pipelines** (SDP / LDP, formerly DLT) demo on
synthetic French retail-energy data. One SQL pipeline takes **5 raw smart-meter files** from a Unity
Catalog Volume all the way to a named, quantified **retention list** — the high-consumption
**Peak Heavy** customers stuck on a **flat tariff** who are already **disputing their bills**.

No Visual Data Prep, no Genie Code, no ML. Just a medallion pipeline: **Auto Loader → bronze →
silver (Expectations) → gold (Materialized Views + one `ai_classify` call)**, governed by Unity
Catalog, deployed by a Databricks Asset Bundle.

> Sibling of the richer `RetailEnergy_DemoPackage_EN` / `retail-energy-handson` demos — same dataset
> and business story, stripped down to the pipeline itself.

## What the demo shows (SDP features)

| Feature | Where |
|---|---|
| **Streaming Table** + **Auto Loader** (`STREAM read_files`, CSV + JSON) | `00-Data-Sources/` bronze |
| **Materialized View** for static reference data | `DLT-Ref-Tariffs.sql` |
| **Expectations** (`CONSTRAINT … EXPECT`, drop / fail / warn) | `01-Silver/` |
| **Nested-JSON flattening** (`struct` field access) | `DLT-Interactions-Silver.sql` |
| **Stream-static join** (stream enriched by a reference MV) | `DLT-Customers-Silver.sql` |
| **Materialized View** aggregations (join + group-by) | `02-Gold/` |
| **`ai_classify`** SQL AI function | `DLT-Gold-Customer-Topics.sql` |
| **CLUSTER BY AUTO** (automatic liquid clustering) | every table |

## Architecture

Open **[docs/energy-architecture.html](docs/energy-architecture.html)** for the full diagram.

```
5 raw files ─► Auto Loader ─► bronze (4 Streaming Tables + ref_tariff_plans MV)
   ─► silver (Expectations, typing, nested-JSON flatten, stream-static tariff join)
   ─► gold  (customer_energy_profile · customer_topics[ai_classify] · at_risk_customers)
   ─► consumed under Unity Catalog governance
```

## Prerequisites

| Requirement | Notes |
|---|---|
| **Databricks workspace** | Unity Catalog enabled, **serverless** compute available |
| **A catalog you can write to** | `CREATE SCHEMA`, `CREATE VOLUME`, `CREATE TABLE` (e.g. `main`, `alp_demo_catalog`) |
| **Foundation Model APIs** | Enabled in the workspace — `ai_classify` (gold topics) calls a built-in model endpoint |
| **Databricks CLI** | Only for the optional CLI deploy path; the UI path needs nothing local |

## Quick install from a Git folder (no local tooling)

The whole demo installs itself from inside the workspace — no local CLI required:

1. **Clone the repo as a Git folder.** In Databricks: **Workspace → Create → Git folder**, URL
   `https://github.com/AlexPicard10/engie-retail-sdp-demo`.
2. **Deploy the bundle.** Open the Git folder's **Deployments** panel (🚀), pick target **dev**,
   set the `catalog` variable to a catalog you can write to (default `alp_demo_catalog`), then
   **Deploy**. This creates the SDP pipeline **and** the `Install_Energy_Retail_SQL_dev` job.
3. **Run the install job.** From the Deployments panel (or **Jobs & Pipelines**) run
   **`Install_Energy_Retail_SQL_dev`**. Two serverless tasks run in order:
   - `generate_data` — creates the schemas + `landing` Volume and writes the 5 synthetic files;
   - `run_pipeline` — triggers the pipeline (bronze → silver → gold). First run is a few-minute
     serverless cold start.
4. **Explore.** Once the job is green, query `<catalog>.engie_retail_en_sql` or run
   [`src/01-Energy_SDP_SQL/03-Explorations/energy_exploration.sql`](src/01-Energy_SDP_SQL/03-Explorations/energy_exploration.sql)
   on a SQL Warehouse.

Re-running the install job is safe: it overwrites the identical raw files (Auto Loader only
ingests genuinely new files) and the pipeline update stays incremental.

> **CLI alternative:** `databricks bundle deploy -t dev --var catalog=<C>` then
> `databricks bundle run install_energy_demo -t dev --var catalog=<C>`. The step-by-step manual
> path is below.

## Walkthrough (step by step)

### 1. Generate the data
Add this repo to your workspace (**Workspace → Create → Git folder**, or drag the folder in). Open
**[`src/00-Utiles/00-Setup-And-Generate-Data.py`](src/00-Utiles/00-Setup-And-Generate-Data.py)**,
set the widgets (`catalog`, `raw_schema`, `schema_sql`, `n_customers`, `mode=load`), attach to
**serverless**, and **Run all**. It creates the schemas + a `landing` Volume and writes the 5 files
into one folder per source.

### 2. Deploy the pipeline
Edit the `catalog` default in [`databricks.yml`](databricks.yml) if needed (it must match step 1),
then deploy the bundle:

- **UI:** Git folder → **Deployments** panel (🚀) → target **dev** → **Deploy**.
- **CLI:**
  ```bash
  databricks bundle validate -t dev --profile <PROFILE>
  databricks bundle deploy   -t dev --profile <PROFILE>
  ```

### 3. Run the pipeline
Open the **`Energy_Retail_SQL_dev`** pipeline and **Start**, or:
```bash
databricks bundle run energy_pipeline_sql -t dev --profile <PROFILE>
```
First serverless run takes a few minutes (cold start) — it builds bronze → silver → gold.

### 4. Explore the results
In `<catalog>.engie_retail_en_sql`, inspect the gold tables, or run
[`src/01-Energy_SDP_SQL/03-Explorations/energy_exploration.sql`](src/01-Energy_SDP_SQL/03-Explorations/energy_exploration.sql)
on a SQL Warehouse: row counts per layer, the segment mix, the `ai_classify` topic breakdown, and
the **`gold_at_risk_customers`** retention list with total revenue at risk.

## Going further (optional)
- **Incremental ingestion:** re-run the setup notebook (it overwrites the files) or drop extra files
  into a `landing/*` folder, then re-run the pipeline — Auto Loader only picks up what's new.
- **Dashboard / Genie:** point an AI/BI dashboard or a Genie space at the three gold tables.

## Teardown
Re-open the setup notebook, set **mode = `teardown`**, **Run all** — it drops the pipeline's output
tables while keeping the schemas, the `landing` Volume and the raw files. Delete the pipeline itself
from the Deployments panel (or `databricks bundle destroy -t dev`).

## Project structure

```
engie_retail_sdponly_demo/
├── README.md                                   ← this file
├── databricks.yml                              ← Asset Bundle (catalog/schema/path variables)
├── docs/
│   └── energy-architecture.html                ← single self-contained architecture diagram
├── resources/
│   ├── energy_pipeline_sql.pipeline.yml         ← serverless SQL pipeline definition
│   └── install_energy_demo.job.yml              ← one-click install job (generate data → run pipeline)
└── src/
    ├── 00-Utiles/
    │   └── 00-Setup-And-Generate-Data.py       ← schemas + Volume + synthetic data generator
    └── 01-Energy_SDP_SQL/                       ← the pipeline (see its own README)
        ├── 00-Data-Sources/  (bronze)
        ├── 01-Silver/        (silver)
        ├── 02-Gold/          (gold)
        └── 03-Explorations/  (validation queries, not in the pipeline)
```

## Disclaimer
All data is **synthetic** (Mimesis + NumPy) and bears no relation to any real customer. Provided
as-is for Databricks demo purposes; not affiliated with any specific energy retailer.
