from __future__ import annotations

import numpy as np
import pandas as pd


class QualityReporter:
    """Compare original and synthetic DataFrames and produce quality reports."""

    def compare(self, original: pd.DataFrame, synthetic: pd.DataFrame) -> dict:
        """Return a quality-comparison dict for a single table pair."""
        report: dict = {}

        # Basic counts
        report["row_count"] = {
            "original": len(original),
            "synthetic": len(synthetic),
        }
        report["column_count"] = {
            "original": len(original.columns),
            "synthetic": len(synthetic.columns),
        }

        # Numeric stats
        num_cols = original.select_dtypes(include="number").columns
        numeric_stats: dict = {}
        for col in num_cols:
            if col in synthetic.columns:
                numeric_stats[col] = {
                    "original": self._describe_numeric(original[col]),
                    "synthetic": self._describe_numeric(synthetic[col]),
                }
        report["numeric_stats"] = numeric_stats

        # Categorical distributions
        cat_cols = original.select_dtypes(include=["object", "category"]).columns
        cat_dist: dict = {}
        for col in cat_cols:
            if col in synthetic.columns:
                cat_dist[col] = {
                    "original": original[col].value_counts().to_dict(),
                    "synthetic": synthetic[col].value_counts().to_dict(),
                }
        report["categorical_distributions"] = cat_dist

        # Correlation difference (numeric columns only)
        if len(num_cols) >= 2:
            common_num = [c for c in num_cols if c in synthetic.columns]
            if len(common_num) >= 2:
                corr_orig = original[common_num].corr()
                corr_syn = synthetic[common_num].corr()
                diff = (corr_orig - corr_syn).abs()
                report["correlation_diff"] = float(diff.mean().mean())
            else:
                report["correlation_diff"] = 0.0
        else:
            report["correlation_diff"] = 0.0

        return report

    def generate_full_report(
        self,
        originals: dict[str, pd.DataFrame],
        synthetics: dict[str, pd.DataFrame],
    ) -> dict:
        """Generate comparison reports for all tables."""
        full_report: dict = {}
        for key in originals:
            if key in synthetics:
                full_report[key] = self.compare(originals[key], synthetics[key])
        return full_report

    @staticmethod
    def _describe_numeric(series: pd.Series) -> dict:
        return {
            "mean": float(series.mean()) if not series.isna().all() else None,
            "std": float(series.std()) if not series.isna().all() else None,
            "min": float(series.min()) if not series.isna().all() else None,
            "max": float(series.max()) if not series.isna().all() else None,
        }
