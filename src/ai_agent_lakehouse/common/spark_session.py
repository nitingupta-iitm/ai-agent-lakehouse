"""Spark session accessor.

On Databricks a `spark` session already exists. For local development / unit
testing we build one configured with Delta Lake so the same transform code runs
in both places.
"""
from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark(app_name: str = "ai-agent-lakehouse") -> SparkSession:
    """Return the active Spark session, creating a Delta-enabled one if needed."""
    active = SparkSession.getActiveSession()
    if active is not None:
        return active

    builder = (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    return builder.getOrCreate()
