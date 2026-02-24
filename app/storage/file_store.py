from __future__ import annotations

import json
import zipfile
from pathlib import Path

import pandas as pd

from app.config import (
    REAL_DATA_DIR,
    RESULTS_DIR,
    SUBMISSIONS_DIR,
    SYNTHETIC_DATA_DIR,
)


class FileStore:
    def __init__(self, base_dir: Path | None = None) -> None:
        self.real_dir = REAL_DATA_DIR
        self.synthetic_dir = SYNTHETIC_DATA_DIR
        self.submissions_dir = SUBMISSIONS_DIR
        self.results_dir = RESULTS_DIR
        if base_dir is not None:
            self.real_dir = base_dir / "real"
            self.synthetic_dir = base_dir / "synthetic"
            self.submissions_dir = base_dir / "submissions"
            self.results_dir = base_dir / "results"

    # ------------------------------------------------------------------
    # Real data
    # ------------------------------------------------------------------

    def save_real_data(
        self,
        dataset_id: str,
        file_type: str,
        file_content: bytes,
        filename: str,
    ) -> Path:
        """Save an uploaded real-data CSV to data_store/real/{dataset_id}/."""
        dest_dir = self.real_dir / dataset_id
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / filename
        dest.write_bytes(file_content)
        return dest

    # ------------------------------------------------------------------
    # Synthetic data
    # ------------------------------------------------------------------

    def save_synthetic_data(
        self,
        dataset_id: str,
        file_type: str,
        df: pd.DataFrame,
    ) -> Path:
        """Save a generated synthetic DataFrame to data_store/synthetic/{dataset_id}/."""
        dest_dir = self.synthetic_dir / dataset_id
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / f"{file_type}.csv"
        df.to_csv(dest, index=False)
        return dest

    # ------------------------------------------------------------------
    # Submissions
    # ------------------------------------------------------------------

    def save_submission_zip(
        self,
        submission_id: str,
        file_content: bytes,
    ) -> Path:
        """Save an uploaded submission ZIP to data_store/submissions/{submission_id}/."""
        dest_dir = self.submissions_dir / submission_id
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / "submission.zip"
        dest.write_bytes(file_content)
        return dest

    def extract_submission(self, zip_path: Path, submission_id: str) -> Path:
        """Extract a submission ZIP and return the extraction directory.

        Validates each member path to prevent zip-slip (path traversal) attacks.
        """
        extract_dir = self.submissions_dir / submission_id / "extracted"
        extract_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zip_path, "r") as zf:
            for member in zf.namelist():
                member_path = (extract_dir / member).resolve()
                if not str(member_path).startswith(str(extract_dir.resolve())):
                    raise ValueError(
                        f"不正なZIPエントリです（パストラバーサル）: {member}"
                    )
            zf.extractall(extract_dir)
        return extract_dir

    # ------------------------------------------------------------------
    # Execution results
    # ------------------------------------------------------------------

    def save_execution_output(
        self,
        execution_id: str,
        stdout: str,
        stderr: str,
        output_json: str | None,
    ) -> tuple[Path, Path, Path | None]:
        """Save execution stdout/stderr/json to data_store/results/{execution_id}/."""
        dest_dir = self.results_dir / execution_id
        dest_dir.mkdir(parents=True, exist_ok=True)

        stdout_path = dest_dir / "stdout.txt"
        stdout_path.write_text(stdout, encoding="utf-8")

        stderr_path = dest_dir / "stderr.txt"
        stderr_path.write_text(stderr, encoding="utf-8")

        json_path: Path | None = None
        if output_json is not None:
            json_path = dest_dir / "output.json"
            json_path.write_text(output_json, encoding="utf-8")

        return stdout_path, stderr_path, json_path

    # ------------------------------------------------------------------
    # CSV helpers
    # ------------------------------------------------------------------

    def read_csv(self, path: Path) -> pd.DataFrame:
        """Read a CSV file into a DataFrame."""
        return pd.read_csv(path)

    # ------------------------------------------------------------------
    # Quality report
    # ------------------------------------------------------------------

    def save_quality_report(self, dataset_id: str, report: dict) -> Path:
        """Save a quality-evaluation report as JSON."""
        dest_dir = self.results_dir / dataset_id
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / "quality_report.json"
        dest.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        return dest

    # ------------------------------------------------------------------
    # Path accessors
    # ------------------------------------------------------------------

    def get_real_data_path(self, dataset_id: str) -> Path:
        """Return the directory path for a dataset's real data."""
        return self.real_dir / dataset_id

    def get_synthetic_data_path(self, dataset_id: str) -> Path:
        """Return the directory path for a dataset's synthetic data."""
        return self.synthetic_dir / dataset_id
