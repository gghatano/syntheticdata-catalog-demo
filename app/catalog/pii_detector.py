from __future__ import annotations

import re

import pandas as pd

# Column-name patterns that suggest PII
_PII_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"(^|_)name($|_)", re.IGNORECASE), "Column name suggests personal name"),
    (re.compile(r"(^|_)(first|last|family|given)(_?name)?($|_)", re.IGNORECASE), "Column name suggests personal name"),
    (re.compile(r"e[-_]?mail", re.IGNORECASE), "Column name suggests email address"),
    (re.compile(r"(^|_)tel(ephone)?($|_)", re.IGNORECASE), "Column name suggests telephone number"),
    (re.compile(r"(^|_)phone($|_)", re.IGNORECASE), "Column name suggests phone number"),
    (re.compile(r"(^|_)address($|_)", re.IGNORECASE), "Column name suggests physical address"),
    (re.compile(r"(^|_)zip(_?code)?($|_)", re.IGNORECASE), "Column name suggests zip code"),
    (re.compile(r"(^|_)birth($|_|day|date)", re.IGNORECASE), "Column name suggests date of birth"),
    (re.compile(r"(^|_)salary($|_)", re.IGNORECASE), "Column name suggests salary information"),
    (re.compile(r"(^|_)(ssn|social_security)", re.IGNORECASE), "Column name suggests SSN"),
    (re.compile(r"(^|_)employee_?id($|_)", re.IGNORECASE), "Column name suggests employee identifier"),
    (re.compile(r"(^|_)passport($|_)", re.IGNORECASE), "Column name suggests passport number"),
    (re.compile(r"(^|_)my_?number($|_)", re.IGNORECASE), "Column name suggests My Number (individual ID)"),
]


def detect_pii_by_name(column_name: str) -> tuple[bool, str | None]:
    """Check if a column name matches known PII patterns.

    Returns (is_pii, reason) tuple.
    """
    for pattern, reason in _PII_PATTERNS:
        if pattern.search(column_name):
            return True, reason
    return False, None


def detect_pii_by_uniqueness(
    series: pd.Series, threshold: float = 0.9, min_rows: int = 10
) -> tuple[bool, str | None]:
    """Detect potential ID-like columns by high uniqueness ratio.

    If > threshold of non-null values are unique, flag as potential PII.
    Only applies to string/object columns with at least min_rows rows.
    """
    if not pd.api.types.is_object_dtype(series.dtype) and not pd.api.types.is_string_dtype(series.dtype):
        return False, None

    non_null = series.dropna()
    if len(non_null) < min_rows:
        return False, None

    unique_ratio = non_null.nunique() / len(non_null)
    if unique_ratio >= threshold:
        return True, f"High uniqueness ratio ({unique_ratio:.1%}) suggests identifier column"
    return False, None


def detect_pii(column_name: str, series: pd.Series) -> tuple[bool, str | None]:
    """Combined PII detection: name heuristics + uniqueness check.

    Returns (is_pii, reason) tuple.
    """
    is_pii, reason = detect_pii_by_name(column_name)
    if is_pii:
        return is_pii, reason

    return detect_pii_by_uniqueness(series)
