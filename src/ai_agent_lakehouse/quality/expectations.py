"""Declarative data-quality expectations.

Each expectation is a named boolean Spark Column that evaluates to True when a
row PASSES the rule. Keeping them declarative makes the rule set easy to read,
extend, and document — and lets us attach the exact failure reason to every
quarantined record.
"""
from __future__ import annotations

from typing import Dict

from pyspark.sql import Column
from pyspark.sql import functions as F

from conf.schemas import VALID_STATUSES


def expectations() -> Dict[str, Column]:
    """Return rule_name -> passing-condition Column.

    Conditions reference the parsed columns produced by the Silver layer
    (`agent_id`, `execution_status`, `duration_ms`).
    """
    return {
        # JSON parsed at all (non-conforming lines parse to all-null structs).
        "valid_json": F.col("agent_id").isNotNull(),
        # Required identity field present.
        "agent_id_present": F.col("agent_id").isNotNull()
        & (F.trim(F.col("agent_id")) != ""),
        # Status is from the controlled vocabulary.
        "status_in_vocabulary": F.col("execution_status").isin(*VALID_STATUSES),
        # Duration present and physically plausible.
        "duration_non_negative": F.col("duration_ms").isNotNull()
        & (F.col("duration_ms") >= 0),
    }


def combined_pass_condition() -> Column:
    """Single Column that is True only when ALL expectations pass."""
    cond: Column = F.lit(True)
    for rule in expectations().values():
        cond = cond & rule
    return cond
