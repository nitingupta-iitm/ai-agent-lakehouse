# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup
# MAGIC Creates the Unity Catalog catalog, the bronze/silver/gold schemas, and the
# MAGIC raw-landing Volume. Idempotent — safe to re-run.

# COMMAND ----------

import sys

sys.path.append("../src")

from ai_agent_lakehouse.common.config import load_config
from ai_agent_lakehouse.common.spark_session import get_spark

# COMMAND ----------

# Optional job parameter: override the catalog per environment.
dbutils.widgets.text("catalog", "")
catalog_override = dbutils.widgets.get("catalog") or None

cfg = load_config(catalog_override=catalog_override)
spark = get_spark()

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {cfg.catalog}")
for schema in cfg.schemas.values():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{schema}")

spark.sql(
    f"CREATE VOLUME IF NOT EXISTS "
    f"{cfg.catalog}.{cfg.schemas['bronze']}.{cfg.volume}"
)

print(f"Catalog ready: {cfg.catalog}")
print(f"Schemas: {list(cfg.schemas.values())}")
print(f"Landing path: {cfg.landing_path}")
