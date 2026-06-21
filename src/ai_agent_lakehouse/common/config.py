"""Load and resolve the pipeline configuration.

Centralizes all naming so transforms never hard-code a catalog/schema/table.
The `catalog` value can be overridden at runtime (e.g. by a Databricks job
parameter) so the same code runs unchanged across dev/staging/prod.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import yaml

# conf/pipeline_config.yml relative to the repo root.
_DEFAULT_CONFIG_PATH = (
    Path(__file__).resolve().parents[3] / "conf" / "pipeline_config.yml"
)


@dataclass(frozen=True)
class PipelineConfig:
    """Resolved configuration with helpers for fully-qualified table names."""

    source_system: str
    catalog: str
    schemas: dict
    volume: str
    landing_subdir: str
    tables: dict
    max_quarantine_ratio: float

    # --- fully-qualified Unity Catalog identifiers -------------------------
    def _fqn(self, layer: str, table_key: str) -> str:
        return f"{self.catalog}.{self.schemas[layer]}.{self.tables[table_key]}"

    @property
    def bronze_table(self) -> str:
        return self._fqn("bronze", "bronze")

    @property
    def silver_table(self) -> str:
        return self._fqn("silver", "silver")

    @property
    def quarantine_table(self) -> str:
        return self._fqn("silver", "quarantine")

    @property
    def gold_table(self) -> str:
        return self._fqn("gold", "gold")

    @property
    def landing_path(self) -> str:
        """UC Volume path where raw files are dropped."""
        return (
            f"/Volumes/{self.catalog}/{self.schemas['bronze']}/"
            f"{self.volume}/{self.landing_subdir}"
        )


def load_config(
    path: Optional[str] = None, catalog_override: Optional[str] = None
) -> PipelineConfig:
    """Read pipeline_config.yml and apply an optional catalog override.

    The catalog override lets the orchestration layer inject a per-environment
    catalog (e.g. ``ai_observability_dev``) without editing the YAML.
    """
    config_path = Path(path) if path else _DEFAULT_CONFIG_PATH
    with open(config_path, "r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)

    catalog = catalog_override or os.environ.get("PIPELINE_CATALOG") or raw["catalog"]

    return PipelineConfig(
        source_system=raw["source_system"],
        catalog=catalog,
        schemas=raw["schemas"],
        volume=raw["volume"],
        landing_subdir=raw["landing_subdir"],
        tables=raw["tables"],
        max_quarantine_ratio=raw["quality"]["max_quarantine_ratio"],
    )
