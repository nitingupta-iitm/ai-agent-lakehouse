# Databricks notebook source
# MAGIC %md
# MAGIC # 99 · Mock Data Generator
# MAGIC Simulates a "file drop" of raw AI-agent execution logs into the landing
# MAGIC Volume — including a deliberately corrupt record so the Silver layer's data
# MAGIC quality + quarantine logic has something to catch.

# COMMAND ----------

import json
import sys

sys.path.append("../src")

from ai_agent_lakehouse.common.config import load_config

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("run_id", "0")  # makes each generated file unique per run
catalog_override = dbutils.widgets.get("catalog") or None
run_id = dbutils.widgets.get("run_id")

cfg = load_config(catalog_override=catalog_override)

# COMMAND ----------

mock_logs = [
    {"agent": "alpha", "status": "success", "duration_ms": 120},
    {"agent": "beta", "status": "failed", "duration_ms": 300},
    {"agent": "alpha", "status": "success", "duration_ms": 115},
    {"agent": "gamma", "status": "success", "duration_ms": 140},
    {"agent": "beta", "status": "timeout", "duration_ms": 5000},
    {"agent": "gamma", "status": "success", "duration_ms": 138},
]

lines = [json.dumps(rec) for rec in mock_logs]
# A corrupt line and a bad-status line — both should be quarantined downstream.
lines.append("CORRUPTED_RECORD_XYZ")
lines.append(json.dumps({"agent": "delta", "status": "??", "duration_ms": -5}))

payload = "\n".join(lines)
out_path = f"{cfg.landing_path}/agent_logs_{run_id}.jsonl"

dbutils.fs.mkdirs(cfg.landing_path)
dbutils.fs.put(out_path, payload, overwrite=True)
print(f"Wrote {len(lines)} raw records to {out_path}")
