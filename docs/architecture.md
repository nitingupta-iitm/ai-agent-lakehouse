# Architecture

## Overview

This project implements the **Medallion Architecture** — the de-facto lakehouse
design pattern — to turn raw, messy AI-agent execution logs into trustworthy,
analytics-ready data. Every layer has one clear responsibility and writes to a
**Delta Lake** table governed by **Unity Catalog**.

```
Raw JSON  ─▶  BRONZE  ─▶  SILVER  ─▶  GOLD  ─▶  BI / Dashboards
(Volume)      raw +       parsed +      per-agent
              metadata    validated     KPIs
                            │
                            └─▶ QUARANTINE (failed records + reasons)
```

## Layer responsibilities

### Bronze — `bronze.agent_logs_raw`
- Reads raw text files from a Unity Catalog **Volume** (`/Volumes/.../raw_landing/agent_logs`).
- Stores the payload verbatim and appends lineage: `ingested_at`, `source_system`, `source_file`.
- **No parsing or filtering** — Bronze is the immutable, replayable system of record.
- Write mode: `append`.

### Silver — `silver.agent_logs_clean` (+ `silver.agent_logs_quarantine`)
- Parses the JSON payload against the **explicit schema** in `conf/schemas.py`.
- Runs declarative **data-quality expectations** (`quality/expectations.py`):
  - `valid_json` — the line parsed into a structured record.
  - `agent_id_present` — required identity field is non-empty.
  - `status_in_vocabulary` — status ∈ {success, failed, timeout}.
  - `duration_non_negative` — duration present and ≥ 0.
- **Quarantines** failing rows (tagged with `dq_failed_rules`) instead of dropping them.
- Fails the run if the quarantine ratio exceeds `max_quarantine_ratio` (default 25%).
- **MERGE/UPSERT** on a deterministic `record_id` → idempotent re-runs.

### Gold — `gold.agent_performance`
- Aggregates Silver into per-agent KPIs: `total_runs`, `success_runs`,
  `success_rate`, `avg_duration_ms`, `p95_duration_ms`, `max_duration_ms`.
- Fully recomputed each run (`overwrite`) — a deterministic projection of Silver.

## Orchestration

A **Databricks Workflow** (`resources/medallion_job.yml`) runs the layers as a
dependent task graph on a shared single-node job cluster:

```
generate_data → setup → bronze_ingest → silver_clean → gold_aggregate
```

Because each task `depends_on` the previous one, a data-quality failure in
`silver_clean` halts the pipeline before bad data reaches Gold, and an email
notification fires on failure.

## Design principles applied

| Principle | Implementation |
|-----------|----------------|
| **Idempotency** | Silver MERGE on `record_id`; Gold overwrite |
| **Schema enforcement** | Explicit `StructType`, no inference |
| **Separation of concerns** | Pure transform functions in `src/`, thin notebooks as entry points |
| **Configuration over code** | All names/thresholds in `conf/pipeline_config.yml` |
| **Observability** | Structured logging + quarantine table with failure reasons |
| **Environment parity** | `catalog` overridable per env via bundle variable / job param |
| **Infrastructure as Code** | Job + code deployed via Databricks Asset Bundle |
```
