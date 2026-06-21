# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze Ingest
# MAGIC Lands raw log files into the Bronze Delta table with lineage metadata.

# COMMAND ----------

import sys

sys.path.append("../src")

from ai_agent_lakehouse.bronze import ingest_agent_logs
from ai_agent_lakehouse.common.config import load_config
from ai_agent_lakehouse.common.spark_session import get_spark

# COMMAND ----------

dbutils.widgets.text("catalog", "")
catalog_override = dbutils.widgets.get("catalog") or None

cfg = load_config(catalog_override=catalog_override)
spark = get_spark()

# COMMAND ----------

rows = ingest_agent_logs.run(spark, cfg)
print(f"Bronze rows ingested: {rows}")

# COMMAND ----------

display(spark.read.table(cfg.bronze_table))
