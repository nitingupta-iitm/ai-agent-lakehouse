# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold Aggregate
# MAGIC Builds the dashboard-ready per-agent performance table from Silver.

# COMMAND ----------

import sys

sys.path.append("../src")

from ai_agent_lakehouse.common.config import load_config
from ai_agent_lakehouse.common.spark_session import get_spark
from ai_agent_lakehouse.gold import agent_performance

# COMMAND ----------

dbutils.widgets.text("catalog", "")
catalog_override = dbutils.widgets.get("catalog") or None

cfg = load_config(catalog_override=catalog_override)
spark = get_spark()

# COMMAND ----------

agents = agent_performance.run(spark, cfg)
print(f"Gold rows (agents): {agents}")

# COMMAND ----------

display(spark.read.table(cfg.gold_table))
