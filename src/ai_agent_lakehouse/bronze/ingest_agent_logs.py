"""BRONZE layer — raw ingestion.

Goal: land data exactly as received, adding only lineage metadata. No parsing,
no filtering. This preserves a faithful, replayable copy of the source.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ai_agent_lakehouse.common.config import PipelineConfig
from ai_agent_lakehouse.common.logging_utils import get_logger

log = get_logger(__name__)


def read_raw(spark: SparkSession, landing_path: str) -> DataFrame:
    """Read raw log lines as text from the landing path (one row per line)."""
    return spark.read.text(landing_path).withColumn(
        "source_file", F.input_file_name()
    )


def to_bronze(df_raw: DataFrame, source_system: str) -> DataFrame:
    """Add ingestion metadata to the raw text payload."""
    return (
        df_raw.withColumnRenamed("value", "raw_payload")
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("source_system", F.lit(source_system))
    )


def run(spark: SparkSession, cfg: PipelineConfig) -> int:
    """Ingest raw files from the Volume into the Bronze Delta table.

    Returns the number of rows written.
    """
    df_raw = read_raw(spark, cfg.landing_path)
    df_bronze = to_bronze(df_raw, cfg.source_system)

    (
        df_bronze.write.format("delta")
        .mode("append")
        .saveAsTable(cfg.bronze_table)
    )

    count = df_bronze.count()
    log.info("Bronze ingest complete: %s rows -> %s", count, cfg.bronze_table)
    return count
