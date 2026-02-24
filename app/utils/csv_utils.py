import pandas as pd
from pathlib import Path


def read_csv(path: Path) -> pd.DataFrame:
    """Read a CSV file and return a DataFrame."""
    return pd.read_csv(path)


def validate_columns(df: pd.DataFrame, required_columns: list[str], file_name: str) -> list[str]:
    """Validate that required columns exist in DataFrame. Returns list of error messages."""
    errors = []
    missing = set(required_columns) - set(df.columns)
    for col in missing:
        errors.append(f"{file_name} の `{col}` 列がありません")
    return errors


def write_csv(df: pd.DataFrame, path: Path) -> None:
    """Write a DataFrame to CSV."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
