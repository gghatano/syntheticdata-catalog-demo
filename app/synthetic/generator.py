from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd


# Japanese family/given names for anonymisation
_FAMILY_NAMES = [
    "佐藤", "鈴木", "高橋", "田中", "伊藤", "渡辺", "中村", "小林",
    "加藤", "吉田", "山田", "松本", "井上", "木村", "林", "斎藤",
    "清水", "森", "池田", "橋本", "阿部", "石川", "前田", "藤田",
    "小川", "岡田", "後藤", "長谷川", "村上", "近藤",
]
_GIVEN_NAMES_M = [
    "太郎", "一郎", "健一", "大輔", "正樹", "拓也", "翔太", "浩二",
    "隆", "誠", "翔", "蓮", "悠真", "陽太", "湊",
]
_GIVEN_NAMES_F = [
    "花子", "美咲", "裕子", "真理", "由美", "恵子", "麻衣", "友香",
    "愛", "奈々", "陽菜", "凛", "結衣", "芽依", "心春",
]


class SyntheticGenerator:
    """MVP synthetic-data generator using simple statistical perturbation."""

    # Columns treated as IDs (will be re-mapped)
    _ID_PATTERNS = ("_id",)
    # Columns treated as personal names (will be replaced)
    _NAME_COLUMNS = ("name",)

    def generate(self, df: pd.DataFrame, seed: int = 42) -> pd.DataFrame:
        """Generate a synthetic DataFrame that preserves statistical properties."""
        rng = np.random.default_rng(seed)
        syn = df.copy()

        for col in syn.columns:
            if self._is_id_column(col):
                syn[col] = self._synthesize_id(syn[col], rng)
            elif col in self._NAME_COLUMNS:
                syn[col] = self._synthesize_name(syn[col], rng)
            elif self._is_date_column(syn[col]):
                syn[col] = self._synthesize_date(syn[col], rng)
            elif pd.api.types.is_numeric_dtype(syn[col]):
                syn[col] = self._synthesize_numeric(syn[col], rng)
            elif pd.api.types.is_categorical_dtype(syn[col]) or pd.api.types.is_object_dtype(syn[col]):
                syn[col] = self._synthesize_categorical(syn[col], rng)

        return syn

    def generate_all(
        self, data_dir: Path, seed: int = 42
    ) -> dict[str, pd.DataFrame]:
        """Read all CSV files from *data_dir* and return synthetic versions."""
        file_keys = ["employee_master", "project_allocation", "working_hours"]
        results: dict[str, pd.DataFrame] = {}
        for key in file_keys:
            csv_path = data_dir / f"{key}.csv"
            if csv_path.exists():
                df = pd.read_csv(csv_path)
                results[key] = self.generate(df, seed=seed)
        return results

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _is_id_column(col: str) -> bool:
        return any(col.endswith(pat) for pat in SyntheticGenerator._ID_PATTERNS)

    @staticmethod
    def _is_date_column(series: pd.Series) -> bool:
        if pd.api.types.is_datetime64_any_dtype(series):
            return True
        if pd.api.types.is_object_dtype(series):
            sample = series.dropna().head(5)
            try:
                pd.to_datetime(sample)
                return True
            except (ValueError, TypeError):
                return False
        return False

    @staticmethod
    def _synthesize_id(series: pd.Series, rng: np.random.Generator) -> pd.Series:
        """Replace IDs with new sequential values, breaking linkage."""
        unique_vals = series.unique()
        # Detect prefix (e.g. "EMP", "PRJ")
        sample = str(unique_vals[0])
        prefix = "".join(c for c in sample if c.isalpha())
        n_digits = len(sample) - len(prefix)

        mapping: dict[str, str] = {}
        shuffled_indices = rng.permutation(len(unique_vals))
        for new_idx, old_val in zip(shuffled_indices, unique_vals):
            mapping[old_val] = f"{prefix}{new_idx + 1:0{n_digits}d}"
        return series.map(mapping)

    @staticmethod
    def _synthesize_name(series: pd.Series, rng: np.random.Generator) -> pd.Series:
        """Replace personal names with random Japanese names."""
        n = len(series)
        family = rng.choice(_FAMILY_NAMES, size=n)
        given = rng.choice(_GIVEN_NAMES_M + _GIVEN_NAMES_F, size=n)
        return pd.Series(
            [f"{f}{g}" for f, g in zip(family, given)],
            index=series.index,
        )

    @staticmethod
    def _synthesize_numeric(series: pd.Series, rng: np.random.Generator) -> pd.Series:
        """Add noise +-10 % while preserving mean / std."""
        if series.isna().all():
            return series
        mean = series.mean()
        std = series.std()
        noise_scale = 0.10
        noise = rng.normal(0, max(std * noise_scale, 1e-9), size=len(series))
        result = series + noise
        # Keep integer dtype when original is integer
        if pd.api.types.is_integer_dtype(series):
            result = result.round().astype(series.dtype)
        return result

    @staticmethod
    def _synthesize_categorical(series: pd.Series, rng: np.random.Generator) -> pd.Series:
        """Shuffle values while preserving the overall distribution."""
        values = series.dropna().values.copy()
        rng.shuffle(values)
        result = series.copy()
        result.loc[result.notna()] = values
        return result

    @staticmethod
    def _synthesize_date(series: pd.Series, rng: np.random.Generator) -> pd.Series:
        """Add a small random offset (+-30 days) to dates."""
        dates = pd.to_datetime(series, errors="coerce")
        offsets = rng.integers(-30, 31, size=len(dates))
        result = dates + pd.to_timedelta(offsets, unit="D")
        # Return in the same string format if original was string
        if pd.api.types.is_object_dtype(series):
            return result.dt.strftime("%Y-%m-%d")
        return result
