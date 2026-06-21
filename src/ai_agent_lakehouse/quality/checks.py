"""Apply data-quality expectations and split a DataFrame into valid / quarantine.

Bad records are never silently dropped: each failing row is tagged with the
list of rules it violated and routed to a quarantine table so issues are
observable and recoverable.
"""
from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ai_agent_lakehouse.quality.expectations import (
    combined_pass_condition,
    expectations,
)


@dataclass
class QualityResult:
    """Output of a quality run: the two partitions plus summary counts."""

    valid: DataFrame
    quarantine: DataFrame
    total: int
    valid_count: int
    quarantine_count: int

    @property
    def quarantine_ratio(self) -> float:
        return self.quarantine_count / self.total if self.total else 0.0


def apply_quality(df: DataFrame) -> QualityResult:
    """Split `df` into rows that pass all expectations and rows that don't.

    Quarantined rows gain a `dq_failed_rules` array column describing exactly
    which expectations failed, which makes triage trivial.
    """
    # Build an array of the names of the rules each row FAILED.
    failed_rules = F.array_remove(
        F.array(
            *[
                F.when(~cond, F.lit(name)).otherwise(F.lit(None))
                for name, cond in expectations().items()
            ]
        ),
        None,
    )

    tagged = df.withColumn("dq_failed_rules", failed_rules)
    pass_cond = combined_pass_condition()

    valid = tagged.filter(pass_cond).drop("dq_failed_rules")
    quarantine = tagged.filter(~pass_cond).withColumn(
        "quarantined_at", F.current_timestamp()
    )

    # Cache before counting so we don't recompute the split three times.
    valid = valid.cache()
    quarantine = quarantine.cache()
    valid_count = valid.count()
    quarantine_count = quarantine.count()

    return QualityResult(
        valid=valid,
        quarantine=quarantine,
        total=valid_count + quarantine_count,
        valid_count=valid_count,
        quarantine_count=quarantine_count,
    )
