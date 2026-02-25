from __future__ import annotations

import numpy as np
import pandas as pd
from scipy import stats as sp_stats
from scipy.spatial.distance import cdist

# ---------- Default thresholds (demo defaults) ----------

DEFAULT_THRESHOLDS = {
    "ks_stat_warn": 0.3,
    "ks_stat_fail": 0.5,
    "wasserstein_warn": 0.3,
    "wasserstein_fail": 0.5,
    "corr_diff_warn": 0.2,
    "corr_diff_fail": 0.4,
    "uniqueness_warn": 0.95,
    "uniqueness_fail": 0.99,
    "k_anonymity_warn": 3,
    "k_anonymity_fail": 1,
    "nearest_neighbor_warn": 0.05,
    "nearest_neighbor_fail": 0.01,
}


class QualityReporter:
    """Compare original and synthetic DataFrames and produce quality reports."""

    def __init__(self, thresholds: dict | None = None):
        self.thresholds = {**DEFAULT_THRESHOLDS, **(thresholds or {})}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def compare(
        self,
        original: pd.DataFrame,
        synthetic: pd.DataFrame,
        pii_columns: list[str] | None = None,
    ) -> dict:
        """Return a quality-comparison dict for a single table pair.

        Backward compatible: existing keys are preserved, new keys are added.
        """
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

        # Numeric stats (legacy)
        num_cols = original.select_dtypes(include="number").columns
        common_num = [c for c in num_cols if c in synthetic.columns]
        numeric_stats: dict = {}
        for col in common_num:
            numeric_stats[col] = {
                "original": self._describe_numeric(original[col]),
                "synthetic": self._describe_numeric(synthetic[col]),
            }
        report["numeric_stats"] = numeric_stats

        # Categorical distributions (legacy)
        cat_cols = original.select_dtypes(include=["object", "category"]).columns
        cat_dist: dict = {}
        for col in cat_cols:
            if col in synthetic.columns:
                cat_dist[col] = {
                    "original": original[col].value_counts().to_dict(),
                    "synthetic": synthetic[col].value_counts().to_dict(),
                }
        report["categorical_distributions"] = cat_dist

        # Correlation difference (legacy)
        report["correlation_diff"] = self._correlation_diff_norm(original, synthetic)

        # ----- New: Utility metrics -----
        utility = {}
        utility["ks_statistics"] = self._ks_statistics(original, synthetic)
        utility["wasserstein_distances"] = self._wasserstein_distances(original, synthetic)
        utility["correlation_diff_norm"] = report["correlation_diff"]
        report["utility"] = utility

        # ----- New: Privacy metrics -----
        qi_columns = self._infer_quasi_identifiers(original, pii_columns)
        privacy = {}
        privacy["uniqueness_ratio"] = self._uniqueness_ratio(synthetic)
        privacy["quasi_identifier_uniqueness"] = self._quasi_identifier_uniqueness(synthetic, qi_columns)
        privacy["nearest_neighbor_distance"] = self._nearest_neighbor_distance(original, synthetic)
        privacy["k_anonymity_proxy"] = self._k_anonymity_proxy(synthetic, qi_columns)
        report["privacy"] = privacy

        # ----- New: Scoring and judgment -----
        utility_score = self._compute_utility_score(utility)
        privacy_score = self._compute_privacy_score(privacy)
        report["scores"] = {
            "utility_score": round(utility_score, 4),
            "privacy_score": round(privacy_score, 4),
        }
        report["judgment"] = self._judge(utility_score, privacy_score)

        return report

    def generate_full_report(
        self,
        originals: dict[str, pd.DataFrame],
        synthetics: dict[str, pd.DataFrame],
        pii_columns_map: dict[str, list[str]] | None = None,
    ) -> dict:
        """Generate comparison reports for all tables."""
        pii_map = pii_columns_map or {}
        full_report: dict = {}
        for key in originals:
            if key in synthetics:
                full_report[key] = self.compare(
                    originals[key], synthetics[key], pii_columns=pii_map.get(key)
                )
        return full_report

    def generate_markdown(self, report: dict) -> str:
        """Generate a Markdown-formatted quality report."""
        lines: list[str] = ["# Quality Report", ""]

        for table_name, table_report in report.items():
            lines.append(f"## {table_name}")
            lines.append("")

            # Row/column counts
            rc = table_report.get("row_count", {})
            lines.append(f"- Rows: original={rc.get('original', '?')}, synthetic={rc.get('synthetic', '?')}")
            cc = table_report.get("column_count", {})
            lines.append(f"- Columns: original={cc.get('original', '?')}, synthetic={cc.get('synthetic', '?')}")
            lines.append("")

            # Utility
            utility = table_report.get("utility", {})
            lines.append("### Utility")
            lines.append("")
            ks = utility.get("ks_statistics", {})
            if ks:
                lines.append("| Column | KS Statistic |")
                lines.append("|--------|-------------|")
                for col, val in ks.items():
                    lines.append(f"| {col} | {val:.4f} |")
                lines.append("")

            wd = utility.get("wasserstein_distances", {})
            if wd:
                lines.append("| Column | Wasserstein Distance |")
                lines.append("|--------|---------------------|")
                for col, val in wd.items():
                    lines.append(f"| {col} | {val:.4f} |")
                lines.append("")

            corr = utility.get("correlation_diff_norm", 0.0)
            lines.append(f"- Correlation diff norm: {corr:.4f}")
            lines.append("")

            # Privacy
            privacy = table_report.get("privacy", {})
            lines.append("### Privacy")
            lines.append("")
            lines.append(f"- Uniqueness ratio: {privacy.get('uniqueness_ratio', '?'):.4f}")
            lines.append(f"- Quasi-identifier uniqueness: {privacy.get('quasi_identifier_uniqueness', '?'):.4f}")
            nn = privacy.get("nearest_neighbor_distance", {})
            if nn:
                lines.append(f"- Nearest neighbor distance: min={nn.get('min', '?'):.4f}, "
                             f"mean={nn.get('mean', '?'):.4f}, median={nn.get('median', '?'):.4f}")
            k_anon = privacy.get("k_anonymity_proxy", None)
            lines.append(f"- k-anonymity proxy (min group size): {k_anon}")
            lines.append("")

            # Scores and judgment
            scores = table_report.get("scores", {})
            judgment = table_report.get("judgment", "N/A")
            lines.append("### Judgment")
            lines.append("")
            lines.append(f"- Utility score: {scores.get('utility_score', '?')}")
            lines.append(f"- Privacy score: {scores.get('privacy_score', '?')}")
            lines.append(f"- **Overall: {judgment}**")
            lines.append("")

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Utility metrics
    # ------------------------------------------------------------------

    @staticmethod
    def _ks_statistics(original: pd.DataFrame, synthetic: pd.DataFrame) -> dict[str, float]:
        """Compute KS statistic for each common numeric column."""
        num_cols = original.select_dtypes(include="number").columns
        result = {}
        for col in num_cols:
            if col not in synthetic.columns:
                continue
            orig_vals = original[col].dropna().values
            syn_vals = synthetic[col].dropna().values
            if len(orig_vals) == 0 or len(syn_vals) == 0:
                result[col] = 1.0
                continue
            stat, _ = sp_stats.ks_2samp(orig_vals, syn_vals)
            result[col] = round(float(stat), 4)
        return result

    @staticmethod
    def _wasserstein_distances(original: pd.DataFrame, synthetic: pd.DataFrame) -> dict[str, float]:
        """Compute Wasserstein distance for each common numeric column (normalized)."""
        num_cols = original.select_dtypes(include="number").columns
        result = {}
        for col in num_cols:
            if col not in synthetic.columns:
                continue
            orig_vals = original[col].dropna().values
            syn_vals = synthetic[col].dropna().values
            if len(orig_vals) == 0 or len(syn_vals) == 0:
                result[col] = 1.0
                continue
            dist = sp_stats.wasserstein_distance(orig_vals, syn_vals)
            # Normalize by range of original
            data_range = float(orig_vals.max() - orig_vals.min())
            if data_range > 0:
                result[col] = round(dist / data_range, 4)
            else:
                result[col] = 0.0
        return result

    @staticmethod
    def _correlation_diff_norm(original: pd.DataFrame, synthetic: pd.DataFrame) -> float:
        """Frobenius norm of correlation matrix difference."""
        num_cols = original.select_dtypes(include="number").columns
        common_num = [c for c in num_cols if c in synthetic.columns]
        if len(common_num) < 2:
            return 0.0
        corr_orig = original[common_num].corr().values.copy()
        corr_syn = synthetic[common_num].corr().values.copy()
        # Replace NaN with 0 for robust computation
        corr_orig = np.nan_to_num(corr_orig, nan=0.0)
        corr_syn = np.nan_to_num(corr_syn, nan=0.0)
        norm = float(np.linalg.norm(corr_orig - corr_syn, "fro"))
        # Normalize by matrix size
        n = len(common_num)
        return round(norm / n, 4)

    # ------------------------------------------------------------------
    # Privacy metrics
    # ------------------------------------------------------------------

    @staticmethod
    def _uniqueness_ratio(synthetic: pd.DataFrame) -> float:
        """Ratio of unique rows in synthetic data."""
        if len(synthetic) == 0:
            return 0.0
        n_unique = len(synthetic.drop_duplicates())
        return round(n_unique / len(synthetic), 4)

    @staticmethod
    def _quasi_identifier_uniqueness(synthetic: pd.DataFrame, qi_columns: list[str]) -> float:
        """Uniqueness ratio of quasi-identifier columns."""
        if not qi_columns or len(synthetic) == 0:
            return 0.0
        valid_qi = [c for c in qi_columns if c in synthetic.columns]
        if not valid_qi:
            return 0.0
        n_unique = len(synthetic[valid_qi].drop_duplicates())
        return round(n_unique / len(synthetic), 4)

    @staticmethod
    def _nearest_neighbor_distance(original: pd.DataFrame, synthetic: pd.DataFrame) -> dict:
        """Compute nearest-neighbor distance summary between original and synthetic numeric rows."""
        num_cols = original.select_dtypes(include="number").columns
        common_num = [c for c in num_cols if c in synthetic.columns]
        if len(common_num) == 0:
            return {"min": 0.0, "mean": 0.0, "median": 0.0}

        orig_vals = original[common_num].dropna().values.astype(float)
        syn_vals = synthetic[common_num].dropna().values.astype(float)

        if len(orig_vals) == 0 or len(syn_vals) == 0:
            return {"min": 0.0, "mean": 0.0, "median": 0.0}

        # Normalize columns by range for fair comparison
        ranges = orig_vals.max(axis=0) - orig_vals.min(axis=0)
        ranges[ranges == 0] = 1.0
        orig_normed = orig_vals / ranges
        syn_normed = syn_vals / ranges

        # Compute pairwise distances and get min per synthetic row
        # For large datasets, use chunked approach
        n_syn = len(syn_normed)
        chunk_size = 500
        min_dists = np.zeros(n_syn)
        for i in range(0, n_syn, chunk_size):
            chunk = syn_normed[i : i + chunk_size]
            dists = cdist(chunk, orig_normed, metric="euclidean")
            min_dists[i : i + chunk_size] = dists.min(axis=1)

        return {
            "min": round(float(min_dists.min()), 4),
            "mean": round(float(min_dists.mean()), 4),
            "median": round(float(np.median(min_dists)), 4),
        }

    @staticmethod
    def _k_anonymity_proxy(synthetic: pd.DataFrame, qi_columns: list[str]) -> int:
        """Minimum group size for quasi-identifier combinations."""
        if not qi_columns or len(synthetic) == 0:
            return 0
        valid_qi = [c for c in qi_columns if c in synthetic.columns]
        if not valid_qi:
            return 0
        group_sizes = synthetic.groupby(valid_qi, observed=True).size()
        return int(group_sizes.min())

    # ------------------------------------------------------------------
    # Scoring and judgment
    # ------------------------------------------------------------------

    def _compute_utility_score(self, utility: dict) -> float:
        """Compute a 0-1 utility score (1 = perfect fidelity)."""
        scores = []

        # KS statistics: average, lower is better -> score = 1 - avg
        ks = utility.get("ks_statistics", {})
        if ks:
            avg_ks = sum(ks.values()) / len(ks)
            scores.append(max(0.0, 1.0 - avg_ks))

        # Wasserstein: average, lower is better -> score = 1 - avg
        wd = utility.get("wasserstein_distances", {})
        if wd:
            avg_wd = sum(wd.values()) / len(wd)
            scores.append(max(0.0, 1.0 - avg_wd))

        # Correlation diff norm: lower is better
        corr = utility.get("correlation_diff_norm", 0.0)
        scores.append(max(0.0, 1.0 - corr))

        if not scores:
            return 0.5
        return sum(scores) / len(scores)

    def _compute_privacy_score(self, privacy: dict) -> float:
        """Compute a 0-1 privacy score (1 = high privacy / low risk).

        Higher nearest-neighbor distance = better privacy.
        Lower quasi-identifier uniqueness = better privacy.
        Higher k-anonymity = better privacy.
        """
        scores = []
        weights = []

        # Nearest neighbor: higher mean distance is better (weight=3, most important)
        nn = privacy.get("nearest_neighbor_distance", {})
        nn_mean = nn.get("mean", 0.0) if nn else 0.0
        # Scale: 0.1 mean distance -> score 0.5, 0.2 -> 1.0
        nn_score = min(1.0, nn_mean * 5)
        scores.append(nn_score)
        weights.append(3.0)

        # k-anonymity: higher is better, cap at 5 for scoring (weight=2)
        k_anon = privacy.get("k_anonymity_proxy", 0)
        if k_anon > 0:
            k_score = min(1.0, k_anon / 5.0)
            scores.append(k_score)
            weights.append(2.0)

        # Quasi-identifier uniqueness: lower is better (weight=1)
        qi_uniq = privacy.get("quasi_identifier_uniqueness", 0.0)
        if qi_uniq > 0:
            scores.append(max(0.0, 1.0 - qi_uniq))
            weights.append(1.0)

        if not scores:
            return 0.5
        return sum(s * w for s, w in zip(scores, weights)) / sum(weights)

    def _judge(self, utility_score: float, privacy_score: float) -> str:
        """Return PASS / WARN / FAIL based on scores."""
        if utility_score >= 0.7 and privacy_score >= 0.5:
            return "PASS"
        if utility_score < 0.4 or privacy_score < 0.2:
            return "FAIL"
        return "WARN"

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _describe_numeric(series: pd.Series) -> dict:
        return {
            "mean": float(series.mean()) if not series.isna().all() else None,
            "std": float(series.std()) if not series.isna().all() else None,
            "min": float(series.min()) if not series.isna().all() else None,
            "max": float(series.max()) if not series.isna().all() else None,
        }

    @staticmethod
    def _infer_quasi_identifiers(
        df: pd.DataFrame, pii_columns: list[str] | None
    ) -> list[str]:
        """Infer quasi-identifier columns.

        If pii_columns is provided, use non-PII categorical columns as QIs.
        Otherwise, use all categorical columns.
        """
        cat_cols = list(df.select_dtypes(include=["object", "category"]).columns)
        if pii_columns:
            return [c for c in cat_cols if c not in pii_columns]
        return cat_cols
