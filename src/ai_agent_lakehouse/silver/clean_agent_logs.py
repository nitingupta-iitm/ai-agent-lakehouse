"""SILVER layer — cleanse, conform, validate, and upsert.

Goal: parse the raw JSON against an explicit schema, run data-quality
expectations, route bad records to quarantine, and MERGE (upsert) the clean
records into the conformed Silver table so re-runs are idempotent.
"""
from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from conf.schemas import AGENT_LOG_SCHEMA
from ai_agent_lakehouse.common.config import PipelineConfig
from ai_agent_lakehouse.common.logging_utils import get_logger
from ai_agent_lakehouse.quality.checks import QualityResult, apply_quality

log = get_logger(__name__)


def parse_payload(df_bronze: DataFrame) -> DataFrame:
    """Parse the raw JSON payload into typed, conformed columns.

    A deterministic business key (`record_id`) is derived so the downstream
    MERGE can upsert idempotently.
    """
    parsed = df_bronze.withColumn(
        "parsed", F.from_json(F.col("raw_payload"), AGENT_LOG_SCHEMA)
    )
    return parsed.select(
        F.col("parsed.agent").alias("agent_id"),
        F.col("parsed.status").alias("execution_status"),
        F.col("parsed.duration_ms").cast("int").alias("duration_ms"),
        F.col("raw_payload"),
        F.col("source_file"),
        F.col("ingested_at"),
        F.sha2(
            F.concat_ws("||", F.col("source_file"), F.col("raw_payload")), 256
        ).alias("record_id"),
    )


def upsert_silver(spark: SparkSession, valid: DataFrame, target_table: str) -> None:
    """MERGE clean records into the Silver Delta table on `record_id`."""
    business_cols = [
        "agent_id",
        "execution_status",
        "duration_ms",
        "raw_payload",
        "source_file",
        "ingested_at",
    ]
    valid_out = valid.select("record_id", *business_cols)

    if not spark.catalog.tableExists(target_table):
        valid_out.write.format("delta").saveAsTable(target_table)
        return

    target = DeltaTable.forName(spark, target_table)
    (
        target.alias("t")
        .merge(valid_out.alias("s"), "t.record_id = s.record_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def run(spark: SparkSession, cfg: PipelineConfig) -> QualityResult:
    """Transform Bronze -> Silver with data-quality gating.

    Raises ValueError if the quarantine ratio exceeds the configured threshold,
    which fails the orchestrated task and signals upstream format breakage.
    """
    df_bronze = spark.read.table(cfg.bronze_table)
    parsed = parse_payload(df_bronze)

    result = apply_quality(parsed)
    log.info(
        "Data quality: %s valid / %s quarantined (%.1f%%) of %s rows",
        result.valid_count,
        result.quarantine_count,
        result.quarantine_ratio * 100,
        result.total,
    )

    # Persist quarantined records for observability and recovery.
    if result.quarantine_count > 0:
        (
            result.quarantine.write.format("delta")
            .mode("append")
            .saveAsTable(cfg.quarantine_table)
        )

    if result.quarantine_ratio > cfg.max_quarantine_ratio:
        raise ValueError(
            f"Quarantine ratio {result.quarantine_ratio:.2%} exceeds threshold "
            f"{cfg.max_quarantine_ratio:.2%} — failing run."
        )

    upsert_silver(spark, result.valid, cfg.silver_table)
    log.info("Silver upsert complete -> %s", cfg.silver_table)
    return result
