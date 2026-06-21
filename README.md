# 🧠 AI Agent Observability Lakehouse

A production-style **Medallion (Bronze → Silver → Gold)** data pipeline on **Databricks + Delta Lake** that ingests raw AI-agent execution logs, cleanses and validates them, and serves business-ready analytics for an observability dashboard.

Built with **Databricks Asset Bundles (DAB)** so the entire pipeline — code, configuration, and the orchestrated job — is version-controlled Infrastructure-as-Code and deploys with a single command.

---

## 🎯 What this project demonstrates

| Skill | Where it shows up |
|-------|-------------------|
| **Medallion architecture** | `src/ai_agent_lakehouse/{bronze,silver,gold}` |
| **Delta Lake** (ACID, MERGE/UPSERT, schema enforcement) | Silver layer `MERGE INTO`, all table writes |
| **Data quality & quarantine** | `src/ai_agent_lakehouse/quality` (bad records isolated, not dropped silently) |
| **Orchestration** | Databricks Workflow defined as code in `resources/medallion_job.yml` |
| **Infrastructure as Code** | `databricks.yml` (Databricks Asset Bundle) |
| **Idempotent, parameterized pipelines** | `conf/pipeline_config.yml` drives catalog/schema/table names |
| **Unity Catalog governance** | 3-level namespace `catalog.schema.table` + Volumes for raw landing |

---

## 🏗️ Architecture

```
                ┌──────────────────────────────────────────────────────────┐
                │                  Databricks Workflow (Job)                 │
                │   setup ──▶ bronze ──▶ silver (+DQ) ──▶ gold               │
                └──────────────────────────────────────────────────────────┘

  Raw JSON logs            BRONZE                 SILVER                  GOLD
  (UC Volume)         (raw + metadata)     (parsed, validated)     (aggregated KPIs)
 ┌───────────┐        ┌──────────────┐     ┌─────────────────┐     ┌──────────────────┐
 │ *.jsonl   │  ───▶  │ agent_logs_  │ ──▶ │ agent_logs_     │ ──▶ │ agent_           │
 │ file drop │        │ raw          │     │ clean (MERGE)   │     │ performance      │
 └───────────┘        └──────────────┘     └─────────────────┘     └──────────────────┘
                                                  │
                                                  ▼  failed DQ / corrupt
                                           ┌─────────────────┐
                                           │ agent_logs_     │
                                           │ quarantine      │
                                           └─────────────────┘
```

- **Bronze** — land data exactly as received, add ingestion metadata (`ingested_at`, `source_system`, `source_file`). No business logic.
- **Silver** — parse JSON against an explicit schema, run **data quality expectations**, route bad records to a **quarantine** table, and **MERGE (upsert)** clean records into the conformed table.
- **Gold** — business aggregations (runs, success rate, avg/percentile latency per agent) ready for BI.

See [docs/architecture.md](docs/architecture.md) for the detailed design.

---

## 📂 Project structure

```
ai-agent-lakehouse/
├── README.md
├── databricks.yml                      # Databricks Asset Bundle (deploys job + code)
├── requirements.txt
├── .gitignore
│
├── conf/
│   ├── pipeline_config.yml             # catalog/schema/table/volume names, params
│   └── schemas.py                      # explicit PySpark schemas for payloads
│
├── resources/
│   └── medallion_job.yml               # Databricks Workflow: setup→bronze→silver→gold
│
├── src/ai_agent_lakehouse/
│   ├── common/
│   │   ├── config.py                   # load + resolve pipeline_config.yml
│   │   ├── spark_session.py            # Databricks-aware Spark accessor
│   │   └── logging_utils.py            # structured logging
│   ├── bronze/ingest_agent_logs.py     # raw ingestion
│   ├── silver/clean_agent_logs.py      # parse, validate, quarantine, MERGE
│   ├── gold/agent_performance.py       # business aggregations
│   └── quality/
│       ├── expectations.py             # declarative DQ rules
│       └── checks.py                   # apply rules, split valid/invalid, report
│
├── notebooks/                          # Databricks notebook entry points (job tasks)
│   ├── 00_setup.py                     # create catalog/schemas/volume
│   ├── 01_bronze_ingest.py
│   ├── 02_silver_clean.py
│   ├── 03_gold_aggregate.py
│   └── 99_data_generator.py            # generate mock agent logs into the Volume
│
├── data/sample/
│   └── agent_logs_sample.jsonl         # tiny local sample (incl. a corrupt record)
│
└── docs/
    └── architecture.md
```

---

## 🚀 Quickstart (Databricks)

> Requires a Databricks workspace (the **free [Community Edition](https://community.cloud.databricks.com/)** works) and the [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/install.html) (`pip install databricks-cli` → v0.205+ for bundles).

```bash
# 1. Authenticate the CLI to your workspace
databricks auth login --host https://<your-workspace>.cloud.databricks.com

# 2. Validate the bundle
databricks bundle validate

# 3. Deploy code + the orchestrated job to your workspace
databricks bundle deploy -t dev

# 4. Run the full medallion pipeline end-to-end
databricks bundle run medallion_pipeline -t dev
```

The job runs the tasks in order: **generate data → setup → bronze → silver (with data quality) → gold**, then you can query `ai_observability.gold.agent_performance`.

### Run without the bundle (any Databricks cluster)
Import the `notebooks/` folder via **Repos** and run them in order `00 → 99 → 01 → 02 → 03`. Every notebook reads its parameters from `conf/pipeline_config.yml`.

---

## 📊 Example Gold output

| agent_id | total_runs | success_runs | success_rate | avg_duration_ms | p95_duration_ms |
|----------|-----------|--------------|--------------|-----------------|-----------------|
| alpha    | 2         | 2            | 1.00         | 117.5           | 120             |
| beta     | 1         | 0            | 0.00         | 300.0           | 300             |
| gamma    | 1         | 1            | 1.00         | 140.0           | 140             |

---

## 🧰 Tech stack

`Databricks` · `Apache Spark (PySpark)` · `Delta Lake` · `Unity Catalog` · `Databricks Asset Bundles` · `Databricks Workflows` · `Python 3.11`

## 📝 License

MIT
