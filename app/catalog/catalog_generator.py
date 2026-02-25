from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from app.catalog.pii_detector import detect_pii
from app.catalog.stats_calculator import calculate_stats
from app.catalog.type_inferrer import infer_types


@dataclass
class CatalogColumnInfo:
    """Intermediate representation of a catalog column before DB persistence."""

    column_name: str
    inferred_type: str
    description: str
    is_pii: bool
    pii_reason: str | None
    stats_json: str


class CatalogGenerator:
    """Generate catalog column metadata from a DataFrame."""

    def generate(self, df: pd.DataFrame) -> list[CatalogColumnInfo]:
        """Analyze a DataFrame and produce catalog column info for each column."""
        type_map = infer_types(df)
        stats_map = calculate_stats(df, type_map)

        columns: list[CatalogColumnInfo] = []
        for col in df.columns:
            inferred_type = type_map[col]
            is_pii, pii_reason = detect_pii(col, df[col])
            description = self._generate_description(col, inferred_type)

            columns.append(
                CatalogColumnInfo(
                    column_name=col,
                    inferred_type=inferred_type,
                    description=description,
                    is_pii=is_pii,
                    pii_reason=pii_reason,
                    stats_json=stats_map[col],
                )
            )
        return columns

    @staticmethod
    def _generate_description(column_name: str, inferred_type: str) -> str:
        """Generate a template description from column name and type."""
        return f"Column '{column_name}' (type: {inferred_type})"
