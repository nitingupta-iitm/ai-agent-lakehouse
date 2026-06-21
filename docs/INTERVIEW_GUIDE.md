# 🎤 Interview Guide — AI Agent Observability Lakehouse

> Read this top to bottom once. By the end you'll be able to explain the project,
> the concepts behind it, and answer the questions an interviewer will actually ask.
> Everything is in plain language, building from zero.

---

## PART 1 — The 60-second pitch (memorize this)

> "I built a data pipeline on **Databricks** that takes raw AI-agent log files,
> cleans and validates them, and produces analytics-ready tables for a dashboard.
> It follows the **Medallion Architecture** — Bronze, Silver, Gold — using **Delta
> Lake** tables. Bad records aren't dropped; they're sent to a **quarantine** table
> so issues are visible. The whole pipeline is **orchestrated** as a Databricks
> Workflow and deployed as **Infrastructure-as-Code** using Databricks Asset
> Bundles."

If you can say that and then explain each underlined term, you're 80% there.

---

## PART 2 — Concepts from scratch (the "why")

### 2.1 What is Data Engineering?
Data engineering is the job of **moving data from where it's created (messy, raw)
to where it's useful (clean, organized, fast to query)**. We build **pipelines** —
automated steps that ingest, transform, and serve data so analysts and dashboards
can trust it.

### 2.2 What is a Data Lake vs Data Warehouse vs Lakehouse?
- **Data Warehouse** — structured, clean tables (like SQL). Great for analytics, but rigid and expensive for raw/unstructured data.
- **Data Lake** — cheap storage (files like JSON/CSV/Parquet in cloud storage, e.g. S3/ADLS). Flexible, but easy to turn into a "data swamp" with no structure or guarantees.
- **Lakehouse** — the best of both: cheap file storage **plus** warehouse-like reliability (ACID transactions, schema, fast SQL). **Databricks + Delta Lake** is the leading lakehouse platform. **This project is a lakehouse.**

### 2.3 What is the Medallion Architecture? (THE core idea)
A way to organize a pipeline into **three quality tiers**, each in its own layer.
Think of it like refining ore into a finished product:

| Layer | Nickname | What it holds | Rule |
|-------|----------|---------------|------|
| 🥉 **Bronze** | Raw | Data exactly as it arrived + tracking info | Never change the data |
| 🥈 **Silver** | Clean | Parsed, validated, deduplicated, conformed | Enforce structure & quality |
| 🥇 **Gold** | Curated | Business aggregations / KPIs | Ready for dashboards |

**Why three layers?** If something breaks, you can always replay from Bronze. Each
layer has ONE job, so the pipeline is easy to debug and trust. This is the single
most important concept in the project — be ready to draw it.

### 2.4 What is Apache Spark / PySpark?
**Spark** is an engine for processing large amounts of data **in parallel across
many machines** (a cluster). **PySpark** is the Python API for Spark. You write code
that looks like it works on one table, and Spark splits the work across the cluster.
A **DataFrame** is Spark's table-like data structure (rows and columns).

Key Spark idea — **lazy evaluation**: Spark doesn't run your transformations
immediately. It builds a plan and only executes when you call an **action** (like
`.count()`, `.show()`, or a write). Transformations like `.select()` and `.filter()`
are "lazy."

### 2.5 What is Delta Lake?
A storage format that sits on top of Parquet files and adds the reliability a data
lake normally lacks:
- **ACID transactions** — writes either fully succeed or fully fail (no half-written data).
- **Schema enforcement** — rejects data that doesn't match the table's columns/types.
- **Time travel** — query previous versions of a table.
- **MERGE / UPSERT** — update existing rows and insert new ones in one operation.
- It keeps a **transaction log** (`_delta_log`) that records every change.

In this project, **every table (Bronze, Silver, Gold) is a Delta table.**

### 2.6 What is Unity Catalog?
Databricks' **governance layer** — it organizes and secures data using a 3-level name:
```
catalog . schema . table
ai_observability . silver . agent_logs_clean
```
- **Catalog** = top-level container (often one per environment: dev/prod).
- **Schema** (a.k.a. database) = a group of tables (we use bronze/silver/gold).
- **Volume** = a governed folder for **files** (where we drop raw logs).

### 2.7 What is Orchestration?
Running pipeline steps **in the right order, on a schedule, with monitoring**. If
step B needs step A's output, the orchestrator guarantees A finishes first, and
alerts you if something fails. Here we use **Databricks Workflows (Jobs)**. (Airflow
is another popular orchestrator — good to mention you know it exists.)

### 2.8 What is Infrastructure as Code (IaC) / Databricks Asset Bundles?
Instead of clicking buttons in a UI to create jobs and clusters, you **describe them
in version-controlled files** (YAML). Then one command deploys everything. This makes
it repeatable, reviewable, and easy to promote from dev → prod. **Databricks Asset
Bundles (DAB)** is the tool for this — our `databricks.yml` + `resources/*.yml`.

### 2.9 What is Data Quality / Quarantine?
Real data is messy. Instead of crashing or silently throwing away bad rows, a good
pipeline **checks each record against rules** ("expectations") and routes failures to
a separate **quarantine** table — tagged with *which rule failed* — so problems are
visible and recoverable.

### 2.10 What is Idempotency? (interviewers love this word)
A pipeline is **idempotent** if running it twice gives the same result as running it
once — no duplicates, no corruption. We achieve this with **MERGE** on a unique key
(Silver) and **overwrite** (Gold).

---

## PART 3 — The project, end to end (the data's journey)

Imagine one log line is born and travels through the pipeline:

```
1. A raw log file lands in a Volume:
   {"agent":"alpha","status":"success","duration_ms":120}
   ...and a bad line:  CORRUPTED_RECORD_XYZ

2. BRONZE: read the lines as-is, add ingested_at, source_system, source_file.
   -> table: ai_observability.bronze.agent_logs_raw

3. SILVER: parse the JSON, run quality rules.
   - Good rows  -> MERGE into ai_observability.silver.agent_logs_clean
   - Bad rows   -> ai_observability.silver.agent_logs_quarantine (with reasons)

4. GOLD: aggregate clean rows per agent (runs, success rate, p95 latency).
   -> table: ai_observability.gold.agent_performance

5. Orchestration: a Databricks Workflow runs steps 1-4 in order, daily,
   and emails on failure.
```

That's the whole story. Now the details per file.

---

## PART 4 — File-by-file walkthrough

### Config layer

**`conf/pipeline_config.yml`** — All the names and settings in one place (catalog,
schema names, table names, the Volume, and the quality threshold). Why? So the code
never hard-codes names — you can point it at a dev or prod catalog without editing code.

**`conf/schemas.py`** — Defines the **explicit schema** of a log record (`agent`,
`status`, `duration_ms`) and the list of valid statuses. Explicit schema = we tell
Spark the exact columns/types instead of letting it guess. This is safer and faster.

**`src/.../common/config.py`** — Loads that YAML into a tidy `PipelineConfig` object
with helper properties like `cfg.silver_table` that build the full
`catalog.schema.table` name for you. The `catalog` can be overridden at runtime so
the same code runs in dev and prod.

**`src/.../common/spark_session.py`** — Gives you a Spark session. On Databricks one
already exists; locally it builds a Delta-enabled one. Lets the same code run anywhere.

**`src/.../common/logging_utils.py`** — Simple logging so the job logs show what happened.

### Bronze layer — `src/.../bronze/ingest_agent_logs.py`
- `read_raw()` — reads the raw text files (one row per line) and records which file each row came from (`input_file_name()` = lineage).
- `to_bronze()` — renames the column to `raw_payload` and adds `ingested_at` (when we loaded it) and `source_system`.
- `run()` — writes to the Bronze Delta table in **append** mode (we keep history; never overwrite raw data).

**Key point:** Bronze does NO parsing. It's a faithful copy + metadata. If anything
downstream breaks, we can reprocess from here.

### Silver layer — `src/.../silver/clean_agent_logs.py`  ← the most important file
- `parse_payload()` — uses `from_json()` with our explicit schema to turn the JSON
  string into real columns (`agent_id`, `execution_status`, `duration_ms`). It also
  builds a **`record_id`** = a SHA-256 hash of `source_file + raw_payload`. This is the
  **unique key** that makes upserts idempotent.
- It calls **`apply_quality()`** (from the quality module) to split rows into valid vs quarantine.
- Quarantined rows are appended to the quarantine table. If too many rows fail
  (over `max_quarantine_ratio`, default 25%), it **raises an error and fails the run**
  — a guard against upstream format breakage.
- `upsert_silver()` — does the **MERGE**: if a `record_id` already exists, update it;
  otherwise insert it. First run (table doesn't exist yet) just creates it.

**This is where you say the words "schema enforcement," "MERGE/upsert," "idempotent,"
and "data quality."**

### Data Quality module — `src/.../quality/`
- **`expectations.py`** — the rules, written declaratively. Each rule is a condition
  that's `True` when the row is good:
  - `valid_json` — the line actually parsed (a corrupt line parses to all-nulls).
  - `agent_id_present` — has a non-empty agent id.
  - `status_in_vocabulary` — status is one of success/failed/timeout.
  - `duration_non_negative` — duration exists and is ≥ 0.
- **`checks.py`** — `apply_quality()` tags each failing row with a `dq_failed_rules`
  list (exactly which rules it broke), then splits the DataFrame into `valid` and
  `quarantine`, and returns counts. It `cache()`s the result so Spark doesn't redo the
  work for each count (small performance optimization worth mentioning).

So `CORRUPTED_RECORD_XYZ` fails `valid_json`, and `{"status":"??","duration_ms":-5}`
fails `status_in_vocabulary` and `duration_non_negative`. Both land in quarantine
with their reasons.

### Gold layer — `src/.../gold/agent_performance.py`
- `build_agent_performance()` — `groupBy("agent_id")` and aggregates:
  `total_runs`, `success_runs`, `success_rate`, `avg_duration_ms`,
  `p95_duration_ms` (95th percentile latency), `max_duration_ms`.
- `run()` — writes Gold in **overwrite** mode (Gold is just a fresh summary of Silver,
  so we rebuild it each time — that's also idempotent).

### Notebooks — `notebooks/`
Thin entry points that Databricks runs as job tasks. They just import the `src`
functions and call `.run()`. Keeping logic in `src/` (not in notebooks) makes it
**reusable and testable**.
- `00_setup.py` — creates the catalog, schemas, and Volume (idempotent `IF NOT EXISTS`).
- `99_data_generator.py` — drops a mock log file (incl. bad records) into the Volume.
- `01/02/03` — run Bronze / Silver / Gold.

### Orchestration — `resources/medallion_job.yml`
Defines the **Databricks Workflow**: tasks `generate_data → setup → bronze_ingest →
silver_clean → gold_aggregate`, each `depends_on` the previous. Runs on a schedule
(cron `0 0 6 * * ?` = 6 AM UTC daily, paused in dev), on a shared single-node cluster,
and emails on failure. Because tasks are chained, if Silver's quality gate fails,
Gold never runs — bad data can't reach the dashboard.

### IaC — `databricks.yml`
The Asset Bundle. Defines the bundle name, a `catalog` variable, and two **targets**
(`dev`, `prod`) with different catalogs. `databricks bundle deploy` pushes code + the
job to the workspace; `databricks bundle run` executes it.

---

## PART 5 — Likely interview questions & strong answers

**Q: Walk me through your project.**
A: Use the 60-second pitch (Part 1), then the data's journey (Part 3).

**Q: Why the Medallion architecture? Why not load straight into final tables?**
A: Separation of concerns and recoverability. Bronze is an immutable raw copy, so I
can always replay. Silver isolates cleaning/validation. Gold is just business logic.
Each layer is independently debuggable, and a failure in one doesn't corrupt the raw data.

**Q: What's the difference between Bronze and Silver?**
A: Bronze is raw text + metadata, no parsing — a faithful system of record. Silver is
parsed against an explicit schema, quality-checked, deduplicated, and upserted.

**Q: How do you handle bad / corrupt data?**
A: I don't drop it silently. I run declarative expectations, tag each failing row with
the exact rules it broke, and route it to a quarantine table. I also fail the run if
the quarantine ratio exceeds a threshold, which flags upstream breakage early.

**Q: What makes your pipeline idempotent? / What if it runs twice?**
A: Silver uses Delta `MERGE` on a deterministic `record_id` (a hash of source file +
payload), so re-running updates rather than duplicates. Gold is a full `overwrite`
recomputed from Silver. So a re-run produces the same result.

**Q: Why Delta Lake instead of plain Parquet?**
A: ACID transactions, schema enforcement, time travel, and MERGE support. Plain
Parquet has none of those guarantees — you can get half-written files and no upserts.

**Q: What is `from_json` doing, and why an explicit schema?**
A: It parses the JSON string column into structured columns. An explicit schema makes
parsing deterministic and lets me detect malformed records (they parse to nulls)
instead of silently inferring wrong types.

**Q: How is it orchestrated? What if a step fails?**
A: A Databricks Workflow runs the tasks as a dependency graph. If a task fails (e.g.
the Silver DQ gate), downstream tasks don't run and I get a failure email. So bad data
never reaches Gold.

**Q: How do you move this from dev to prod?**
A: It's deployed via Databricks Asset Bundles. I have `dev` and `prod` targets with
different catalogs. The same code deploys to either with `databricks bundle deploy -t prod`.

**Q: What is lazy evaluation in Spark?**
A: Transformations (select/filter/groupBy) just build a plan; Spark only executes when
an action (count/show/write) is called. This lets Spark optimize the whole plan.

**Q: What's a transformation vs an action?**
A: Transformation = returns a new DataFrame, lazy (`select`, `filter`, `withColumn`,
`groupBy`). Action = triggers execution and returns a result or writes (`count`,
`show`, `collect`, `write`).

**Q: Why `cache()` in the quality check?**
A: After splitting valid/quarantine I call `.count()` on both. Without caching, Spark
would recompute the split twice. Caching keeps the computed result in memory.

**Q: What's the p95 latency and why use it over average?**
A: The 95th percentile — 95% of runs were faster than this. Averages hide outliers;
p95 captures tail latency, which matters for performance SLAs.

**Q: How would you scale this to millions of records / streaming?**
A: Spark already scales horizontally across the cluster. For continuous data I'd use
**Auto Loader** / **Structured Streaming** to incrementally ingest new files into
Bronze, and run Silver/Gold incrementally. I'd also partition large tables (e.g. by
date) and use `OPTIMIZE`/`Z-ORDER` on Delta for query performance.

**Q: How would you add tests?**
A: Because transforms are pure functions on DataFrames in `src/`, I can unit-test them
with a local SparkSession (pytest), feeding small DataFrames and asserting on the
output — e.g. that a bad status lands in quarantine.

---

## PART 6 — Glossary (rapid-fire)

- **ETL / ELT** — Extract-Transform-Load. We do ELT: load raw first (Bronze), transform inside the lakehouse.
- **DataFrame** — Spark's distributed table.
- **Schema** — the columns and their types.
- **Schema enforcement** — rejecting/handling data that doesn't match the schema.
- **Partition** — splitting a table's files by a column (e.g. date) to read less data.
- **MERGE / UPSERT** — update-or-insert in one operation.
- **Idempotent** — re-running is safe; same result.
- **ACID** — Atomicity, Consistency, Isolation, Durability (transaction guarantees).
- **Lineage** — knowing where each row came from (`source_file`, `ingested_at`).
- **Quarantine** — a holding table for records that fail quality checks.
- **DAG** — Directed Acyclic Graph; the shape of an orchestrated task dependency graph.
- **Cron** — schedule syntax (`0 0 6 * * ?` = daily 6 AM).
- **Unity Catalog** — Databricks governance: `catalog.schema.table` + Volumes.
- **Volume** — governed storage for files in Unity Catalog.
- **Asset Bundle (DAB)** — Databricks Infrastructure-as-Code.

---

## PART 7 — If you only have 5 minutes before the interview

Memorize these 7 sentences:
1. It's a **Medallion (Bronze→Silver→Gold)** pipeline on **Databricks + Delta Lake**.
2. **Bronze** = raw data + metadata, never modified.
3. **Silver** = parsed with an explicit schema, quality-checked, bad rows **quarantined**, good rows **MERGE-upserted** (idempotent).
4. **Gold** = per-agent business KPIs, recomputed each run.
5. **Delta Lake** gives ACID, schema enforcement, time travel, and MERGE.
6. It's **orchestrated** by a Databricks Workflow (tasks run in order, alert on failure).
7. It's deployed as **Infrastructure-as-Code** via Databricks Asset Bundles, with dev/prod targets.

Good luck — you've got this. 🚀
