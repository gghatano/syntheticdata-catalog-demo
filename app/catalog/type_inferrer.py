from __future__ import annotations

import pandas as pd


def infer_column_type(series: pd.Series) -> str:
    """Infer a simplified type from a pandas Series.

    Returns one of: string, int, float, date, bool.
    """
    if series.dropna().empty:
        return "string"

    dtype = series.dtype

    if pd.api.types.is_bool_dtype(dtype):
        return "bool"

    if pd.api.types.is_integer_dtype(dtype):
        return "int"

    if pd.api.types.is_float_dtype(dtype):
        # Check if all non-null values are actually integers stored as float
        non_null = series.dropna()
        if (non_null == non_null.astype(int)).all():
            return "int"
        return "float"

    # For object/string columns, try to parse as date
    if pd.api.types.is_object_dtype(dtype) or pd.api.types.is_string_dtype(dtype):
        non_null = series.dropna()
        if len(non_null) > 0:
            # Try bool detection (e.g. "True"/"False", "yes"/"no")
            lower_vals = set(non_null.astype(str).str.lower().unique())
            if lower_vals <= {"true", "false", "yes", "no", "1", "0"}:
                return "bool"

            # Try date detection
            try:
                pd.to_datetime(non_null, format="mixed")
                return "date"
            except (ValueError, TypeError):
                pass

        return "string"

    return "string"


def infer_types(df: pd.DataFrame) -> dict[str, str]:
    """Return a mapping of column name -> inferred type for all columns."""
    return {col: infer_column_type(df[col]) for col in df.columns}
