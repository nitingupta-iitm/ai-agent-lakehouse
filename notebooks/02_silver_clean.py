# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver Clean
# MAGIC Parses JSON, runs data-quality expectations, quarantines bad records, and
# MAGIC MERGEs clean records into the conformed Silver table.

# COMMAND ----------

import sys

sys.path.append("../src")

from ai_agent_lakehouse.common.config import load_config
from ai_agent_lakehouse.common.spark_session import get_spark
from ai_agent_lakehouse.silver import clean_agent_logs

# COMMAND ----------

dbutils.widgets.text("catalog", "")
catalog_override = dbutils.widgets.get("catalog") or None

cfg = load_config(catalog_override=catalog_override)
spark = get_spark()

# COMMAND ----------

result = clean_agent_logs.run(spark, cfg)
print(
    f"Valid: {result.valid_count} | "
    f"Quarantined: {result.quarantine_count} "
    f"({result.quarantine_ratio:.1%})"
)

# COMMAND ----------

# MAGIC %md ### Clean (Silver) records
display(spark.read.table(cfg.silver_table))

# COMMAND ----------

# MAGIC %md ### Quarantined records (with failure reasons)
display(spark.read.table(cfg.quarantine_table))
