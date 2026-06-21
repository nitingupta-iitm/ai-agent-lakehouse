"""Explicit schemas for incoming payloads.

Defining schemas up front (rather than inferring) is a core data-engineering
discipline: it makes parsing deterministic, prevents silent type drift, and lets
us cleanly separate well-formed records from corrupt ones.
"""
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Schema of a single AI-agent execution log line (the JSON payload).
AGENT_LOG_SCHEMA = StructType(
    [
        StructField("agent", StringType(), nullable=True),
        StructField("status", StringType(), nullable=True),
        StructField("duration_ms", IntegerType(), nullable=True),
    ]
)

# Allowed values for the `status` field — anything else is a quality failure.
VALID_STATUSES = ("success", "failed", "timeout")
