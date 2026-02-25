from __future__ import annotations

import numpy as np
import pandas as pd

from app.synthetic.quality_report import QualityReporter


# ---------- Fixtures ----------


def _make_original() -> pd.DataFrame:
    rng = np.random.default_rng(42)
    return pd.DataFrame({
        "employee_id": [f"EMP{i:03d}" for i in range(100)],
        "name": [f"Person{i}" for i in range(100)],
        "department": rng.choice(["Eng", "Sales", "HR", "Finance"], size=100),
        "salary": rng.integers(400000, 800000, size=100),
        "bonus": rng.integers(50000, 200000, size=100),
        "age": rng.integers(22, 60, size=100),
    })


def _make_synthetic_good(original: pd.DataFrame) -> pd.DataFrame:
    """Synthetic data that closely resembles original (small perturbation)."""
    rng = np.random.default_rng(99)
    syn = original.copy()
    syn["employee_id"] = [f"SYN{i:03d}" for i in range(len(syn))]
    syn["name"] = [f"Synth{i}" for i in range(len(syn))]
    # Small noise on numeric columns
    for col in ["salary", "bonus", "age"]:
        noise = rng.normal(0, original[col].std() * 0.05, size=len(syn))
        syn[col] = (syn[col] + noise).round().astype(int)
    # Shuffle departments
    dept_vals = syn["department"].values.copy()
    rng.shuffle(dept_vals)
    syn["department"] = dept_vals
    return syn


def _make_synthetic_bad() -> pd.DataFrame:
    """Synthetic data that is very different from the original."""
    rng = np.random.default_rng(123)
    return pd.DataFrame({
        "employee_id": [f"BAD{i:03d}" for i in range(100)],
        "name": [f"Bad{i}" for i in range(100)],
        "department": rng.choice(["X", "Y"], size=100),
        "salary": rng.integers(100, 200, size=100),
        "bonus": rng.integers(1, 10, size=100),
        "age": rng.integers(80, 99, size=100),
    })


# ---------- KS Statistic Tests ----------


class TestKSStatistics:
    def test_identical_data_ks_zero(self):
        orig = _make_original()
        reporter = QualityReporter()
        ks = reporter._ks_statistics(orig, orig.copy())
        for val in ks.values():
            assert val == 0.0

    def test_similar_data_ks_low(self):
        orig = _make_original()
        syn = _make_synthetic_good(orig)
        reporter = QualityReporter()
        ks = reporter._ks_statistics(orig, syn)
        for val in ks.values():
            assert val < 0.5

    def test_different_data_ks_high(self):
        orig = _make_original()
        syn = _make_synthetic_bad()
        reporter = QualityReporter()
        ks = reporter._ks_statistics(orig, syn)
        # At least one column should have high KS
        assert max(ks.values()) > 0.5


# ---------- Wasserstein Distance Tests ----------


class TestWassersteinDistances:
    def test_identical_data_wasserstein_zero(self):
        orig = _make_original()
        reporter = QualityReporter()
        wd = reporter._wasserstein_distances(orig, orig.copy())
        for val in wd.values():
            assert val == 0.0

    def test_similar_data_wasserstein_low(self):
        orig = _make_original()
        syn = _make_synthetic_good(orig)
        reporter = QualityReporter()
        wd = reporter._wasserstein_distances(orig, syn)
        for val in wd.values():
            assert val < 0.3


# ---------- Correlation Diff Tests ----------


class TestCorrelationDiff:
    def test_identical_data_corr_zero(self):
        orig = _make_original()
        reporter = QualityReporter()
        diff = reporter._correlation_diff_norm(orig, orig.copy())
        assert diff == 0.0

    def test_single_numeric_column(self):
        df = pd.DataFrame({"a": [1, 2, 3]})
        reporter = QualityReporter()
        diff = reporter._correlation_diff_norm(df, df.copy())
        assert diff == 0.0


# ---------- Privacy Metric Tests ----------


class TestPrivacyMetrics:
    def test_uniqueness_ratio_all_unique(self):
        df = pd.DataFrame({"a": range(100), "b": range(100, 200)})
        reporter = QualityReporter()
        ratio = reporter._uniqueness_ratio(df)
        assert ratio == 1.0

    def test_uniqueness_ratio_duplicates(self):
        df = pd.DataFrame({"a": [1, 1, 2, 2], "b": [3, 3, 4, 4]})
        reporter = QualityReporter()
        ratio = reporter._uniqueness_ratio(df)
        assert ratio == 0.5

    def test_quasi_identifier_uniqueness(self):
        df = pd.DataFrame({
            "dept": ["Eng", "Sales", "Eng", "Sales"],
            "level": ["Jr", "Jr", "Sr", "Sr"],
        })
        reporter = QualityReporter()
        ratio = reporter._quasi_identifier_uniqueness(df, ["dept", "level"])
        assert ratio == 1.0

    def test_quasi_identifier_uniqueness_no_qi(self):
        df = pd.DataFrame({"a": [1, 2, 3]})
        reporter = QualityReporter()
        ratio = reporter._quasi_identifier_uniqueness(df, [])
        assert ratio == 0.0

    def test_k_anonymity_proxy(self):
        df = pd.DataFrame({
            "dept": ["Eng", "Eng", "Sales", "Sales", "Sales"],
        })
        reporter = QualityReporter()
        k = reporter._k_anonymity_proxy(df, ["dept"])
        assert k == 2  # Eng group has 2

    def test_k_anonymity_proxy_no_qi(self):
        df = pd.DataFrame({"a": [1, 2, 3]})
        reporter = QualityReporter()
        k = reporter._k_anonymity_proxy(df, [])
        assert k == 0

    def test_nearest_neighbor_nonzero_for_different_data(self):
        orig = _make_original()
        syn = _make_synthetic_bad()
        reporter = QualityReporter()
        nn = reporter._nearest_neighbor_distance(orig, syn)
        assert nn["mean"] > 0
        assert nn["min"] >= 0

    def test_nearest_neighbor_zero_for_identical(self):
        orig = _make_original()
        reporter = QualityReporter()
        nn = reporter._nearest_neighbor_distance(orig, orig.copy())
        assert nn["min"] == 0.0
        assert nn["mean"] == 0.0


# ---------- Scoring and Judgment Tests ----------


class TestScoringAndJudgment:
    def test_good_synthetic_has_high_utility(self):
        orig = _make_original()
        syn = _make_synthetic_good(orig)
        reporter = QualityReporter()
        report = reporter.compare(orig, syn)
        assert report["scores"]["utility_score"] > 0.5

    def test_bad_synthetic_fails_or_warns(self):
        orig = _make_original()
        syn = _make_synthetic_bad()
        reporter = QualityReporter()
        report = reporter.compare(orig, syn)
        assert report["judgment"] in ("WARN", "FAIL")

    def test_identical_data_has_perfect_utility(self):
        orig = _make_original()
        reporter = QualityReporter()
        report = reporter.compare(orig, orig.copy())
        assert report["scores"]["utility_score"] == 1.0

    def test_identical_data_has_low_privacy(self):
        """Identical data should have poor privacy (it IS the original)."""
        orig = _make_original()
        reporter = QualityReporter()
        report = reporter.compare(orig, orig.copy())
        assert report["scores"]["privacy_score"] < 0.3
        assert report["judgment"] == "FAIL"

    def test_judgment_thresholds_pass(self):
        reporter = QualityReporter()
        assert reporter._judge(0.8, 0.6) == "PASS"

    def test_judgment_thresholds_warn(self):
        reporter = QualityReporter()
        assert reporter._judge(0.5, 0.3) == "WARN"

    def test_judgment_thresholds_fail_low_utility(self):
        reporter = QualityReporter()
        assert reporter._judge(0.3, 0.6) == "FAIL"

    def test_judgment_thresholds_fail_low_privacy(self):
        reporter = QualityReporter()
        assert reporter._judge(0.8, 0.1) == "FAIL"


# ---------- Full Report and Markdown Tests ----------


class TestFullReportAndMarkdown:
    def test_generate_full_report_structure(self):
        orig = _make_original()
        syn = _make_synthetic_good(orig)
        reporter = QualityReporter()
        report = reporter.generate_full_report(
            {"employee_master": orig},
            {"employee_master": syn},
        )
        assert "employee_master" in report
        table_report = report["employee_master"]
        assert "utility" in table_report
        assert "privacy" in table_report
        assert "scores" in table_report
        assert "judgment" in table_report

    def test_generate_full_report_with_pii(self):
        orig = _make_original()
        syn = _make_synthetic_good(orig)
        reporter = QualityReporter()
        report = reporter.generate_full_report(
            {"employee_master": orig},
            {"employee_master": syn},
            pii_columns_map={"employee_master": ["name", "employee_id"]},
        )
        assert "employee_master" in report

    def test_generate_markdown(self):
        orig = _make_original()
        syn = _make_synthetic_good(orig)
        reporter = QualityReporter()
        report = reporter.generate_full_report(
            {"employee_master": orig},
            {"employee_master": syn},
        )
        md = reporter.generate_markdown(report)
        assert "# Quality Report" in md
        assert "## employee_master" in md
        assert "### Utility" in md
        assert "### Privacy" in md
        assert "### Judgment" in md
        assert "PASS" in md or "WARN" in md or "FAIL" in md

    def test_backward_compatibility_keys(self):
        """Ensure legacy keys still exist in the report."""
        orig = _make_original()
        syn = _make_synthetic_good(orig)
        reporter = QualityReporter()
        report = reporter.compare(orig, syn)
        assert "row_count" in report
        assert "column_count" in report
        assert "numeric_stats" in report
        assert "categorical_distributions" in report
        assert "correlation_diff" in report
