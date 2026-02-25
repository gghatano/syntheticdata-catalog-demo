from __future__ import annotations

import json

import pandas as pd


def calculate_column_stats(series: pd.Series, inferred_type: str) -> dict:
    """Calculate summary statistics for a single column.

    Returns a dict with: missing_rate, unique_count, and type-appropriate stats.
    """
    total = len(series)
    missing = int(series.isna().sum())
    missing_rate = missing / total if total > 0 else 0.0
    unique_count = int(series.nunique())

    stats: dict = {
        "missing_rate": round(missing_rate, 4),
        "missing_count": missing,
        "total_count": total,
        "unique_count": unique_count,
    }

    non_null = series.dropna()
    if len(non_null) == 0:
        return stats

    if inferred_type in ("int", "float"):
        numeric = pd.to_numeric(non_null, errors="coerce").dropna()
        if len(numeric) > 0:
            stats["min"] = _safe_scalar(numeric.min())
            stats["max"] = _safe_scalar(numeric.max())
            stats["mean"] = round(float(numeric.mean()), 4)

    if inferred_type == "date":
        try:
            dates = pd.to_datetime(non_null, errors="coerce").dropna()
            if len(dates) > 0:
                stats["min"] = str(dates.min().date())
                stats["max"] = str(dates.max().date())
        except (ValueError, TypeError):
            pass

    # Mode (most frequent value) - applicable to all types
    mode_vals = non_null.mode()
    if len(mode_vals) > 0:
        stats["mode"] = str(mode_vals.iloc[0])

    return stats


def calculate_stats(df: pd.DataFrame, type_map: dict[str, str]) -> dict[str, str]:
    """Calculate stats for all columns and return as JSON strings.

    Returns a dict of column_name -> JSON string.
    """
    result = {}
    for col in df.columns:
        inferred_type = type_map.get(col, "string")
        stats = calculate_column_stats(df[col], inferred_type)
        result[col] = json.dumps(stats, ensure_ascii=False)
    return result


def _safe_scalar(value):
    """Convert numpy scalar to Python native type."""
    try:
        return value.item()
    except AttributeError:
        return value
