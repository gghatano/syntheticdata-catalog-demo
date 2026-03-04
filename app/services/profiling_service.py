from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models import CatalogColumn, Dataset, SyntheticArtifact, User, UserRole
from app.storage.file_store import FileStore


class ProfilingService:
    def __init__(self, db: Session, file_store: FileStore | None = None):
        self.db = db
        self.file_store = file_store

    def get_profile_data(self, dataset_id: str, actor_user_id: str) -> list[dict]:
        """Read CatalogColumn + synthetic CSV to generate rich profile data."""
        actor = self._get_user_or_raise(actor_user_id)
        dataset = self._get_dataset_or_raise(dataset_id)

        if actor.role != UserRole.hr and not dataset.is_published:
            raise PermissionError(
                f"User '{actor_user_id}' has no access to dataset '{dataset_id}'"
            )

        columns = list(
            self.db.execute(
                select(CatalogColumn).where(CatalogColumn.dataset_id == dataset.id)
            ).scalars().all()
        )

        # Try to read synthetic CSV for richer profiling
        df = self._read_synthetic_csv(dataset)

        profiles = []
        for col in columns:
            stats = json.loads(col.stats_json) if col.stats_json else {}
            series = df[col.column_name] if df is not None and col.column_name in df.columns else None

            profile: dict = {
                "column_name": col.column_name,
                "inferred_type": col.inferred_type,
                "is_pii": col.is_pii,
                "total_count": int(series.shape[0]) if series is not None else stats.get("total_count", 0),
                "null_count": int(series.isna().sum()) if series is not None else stats.get("missing_count", 0),
                "unique_count": int(series.nunique()) if series is not None else stats.get("unique_count", 0),
            }

            total = profile["total_count"]
            profile["null_rate"] = round(profile["null_count"] / total, 4) if total > 0 else 0.0

            if col.inferred_type in ("int64", "float64", "numeric", "integer", "float"):
                profile["chart_type"] = "histogram"
                profile["chart_data"] = self._build_numeric_profile(series, stats)
            else:
                profile["chart_type"] = "bar"
                profile["chart_data"] = self._build_categorical_profile(series, stats)

            profiles.append(profile)

        return profiles

    def _read_synthetic_csv(self, dataset: Dataset) -> pd.DataFrame | None:
        """Read the first synthetic CSV artifact for profiling."""
        artifacts = list(
            self.db.execute(
                select(SyntheticArtifact).where(SyntheticArtifact.dataset_id == dataset.id)
            ).scalars().all()
        )
        for artifact in artifacts:
            path = Path(artifact.file_path)
            if path.exists() and path.suffix == ".csv":
                try:
                    return pd.read_csv(path)
                except Exception:
                    continue
        return None

    def _build_numeric_profile(self, series: pd.Series | None, stats: dict) -> dict:
        """Build histogram + stats from CSV series, falling back to stats_json."""
        if series is not None:
            numeric = pd.to_numeric(series, errors="coerce").dropna()
            if len(numeric) > 0:
                histogram = self._compute_histogram(numeric)
                return {
                    "labels": histogram["labels"],
                    "values": histogram["values"],
                    "stats": {
                        "min": float(numeric.min()),
                        "max": float(numeric.max()),
                        "mean": round(float(numeric.mean()), 2),
                        "std": round(float(numeric.std()), 2) if len(numeric) > 1 else 0.0,
                        "median": float(numeric.median()),
                        "p25": float(np.percentile(numeric, 25)),
                        "p75": float(np.percentile(numeric, 75)),
                    },
                }

        # Fallback to stats_json
        labels = []
        values = []
        if "min" in stats and "max" in stats:
            labels = ["min", "mean", "max"]
            values = [stats.get("min", 0), stats.get("mean", 0), stats.get("max", 0)]
        return {
            "labels": labels,
            "values": values,
            "stats": {k: stats[k] for k in ("min", "max", "mean", "std") if k in stats},
        }

    def _build_categorical_profile(self, series: pd.Series | None, stats: dict) -> dict:
        """Build value_counts bar chart from CSV series, falling back to stats_json."""
        if series is not None:
            vc = self._compute_value_counts(series)
            return {
                "labels": vc["labels"],
                "values": vc["values"],
                "stats": {
                    "cardinality": int(series.nunique()),
                },
            }

        # Fallback to stats_json
        labels = []
        values = []
        if "value_counts" in stats:
            vc_data = stats["value_counts"]
            if isinstance(vc_data, dict):
                for k, v in sorted(vc_data.items(), key=lambda x: -x[1])[:10]:
                    labels.append(str(k))
                    values.append(v)
        return {
            "labels": labels,
            "values": values,
            "stats": {k: stats[k] for k in ("unique_count", "count") if k in stats},
        }

    @staticmethod
    def _compute_histogram(series: pd.Series, bins: int = 10) -> dict:
        """Compute histogram with N bins using numpy."""
        counts, bin_edges = np.histogram(series, bins=bins)
        labels = [f"{bin_edges[i]:.1f}-{bin_edges[i + 1]:.1f}" for i in range(len(counts))]
        return {"labels": labels, "values": [int(c) for c in counts]}

    @staticmethod
    def _compute_value_counts(series: pd.Series, top_n: int = 10) -> dict:
        """Compute top-N value counts."""
        vc = series.value_counts().head(top_n)
        return {"labels": [str(v) for v in vc.index], "values": [int(c) for c in vc.values]}

    def _get_user_or_raise(self, user_id: str) -> User:
        user = self.db.execute(
            select(User).where(User.user_id == user_id)
        ).scalar_one_or_none()
        if user is None:
            raise ValueError(f"User not found: {user_id}")
        return user

    def _get_dataset_or_raise(self, dataset_id: str) -> Dataset:
        dataset = self.db.execute(
            select(Dataset).where(Dataset.dataset_id == dataset_id)
        ).scalar_one_or_none()
        if dataset is None:
            raise ValueError(f"Dataset not found: {dataset_id}")
        return dataset
