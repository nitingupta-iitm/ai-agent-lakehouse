"""GOLD layer — business aggregations.

Goal: produce dashboard-ready KPIs per agent (volume, reliability, latency).
The Gold table is fully recomputed each run (overwrite) since it is a
deterministic projection of Silver.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ai_agent_lakehouse.common.config import PipelineConfig
from ai_agent_lakehouse.common.logging_utils import get_logger

log = get_logger(__name__)


def build_agent_performance(df_silver: DataFrame) -> DataFrame:
    """Aggregate clean agent logs into per-agent performance metrics."""
    is_success = (F.col("execution_status") == "success").cast("int")
    return (
        df_silver.groupBy("agent_id")
        .agg(
            F.count("*").alias("total_runs"),
            F.sum(is_success).alias("success_runs"),
            F.round(F.avg("duration_ms"), 2).alias("avg_duration_ms"),
            F.expr("percentile_approx(duration_ms, 0.95)").alias("p95_duration_ms"),
            F.max("duration_ms").alias("max_duration_ms"),
        )
        .withColumn(
            "success_rate",
            F.round(F.col("success_runs") / F.col("total_runs"), 2),
        )
        .withColumn("computed_at", F.current_timestamp())
        .select(
            "agent_id",
            "total_runs",
            "success_runs",
            "success_rate",
            "avg_duration_ms",
            "p95_duration_ms",
            "max_duration_ms",
            "computed_at",
        )
        .orderBy(F.col("total_runs").desc())
    )


def run(spark: SparkSession, cfg: PipelineConfig) -> int:
    """Recompute the Gold performance table from Silver. Returns row count."""
    df_silver = spark.read.table(cfg.silver_table)
    df_gold = build_agent_performance(df_silver)

    (
        df_gold.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(cfg.gold_table)
    )

    count = df_gold.count()
    log.info("Gold aggregation complete: %s agents -> %s", count, cfg.gold_table)
    return count
