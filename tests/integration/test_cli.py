from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


def _run_cli(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    """Run the CLI as a subprocess."""
    return subprocess.run(
        [sys.executable, "-m", "app.cli", *args],
        capture_output=True,
        text=True,
        cwd=str(PROJECT_ROOT),
        check=check,
    )


class TestCliUsersSeed:
    def test_users_seed_succeeds(self, tmp_path: Path):
        result = _run_cli("users", "seed")
        assert result.returncode == 0
        assert "デモユーザー" in result.stdout

    def test_users_seed_idempotent(self, tmp_path: Path):
        _run_cli("users", "seed")
        result = _run_cli("users", "seed")
        assert result.returncode == 0


class TestCliDemoSeedData:
    def test_demo_seed_data_succeeds(self, tmp_path: Path):
        result = _run_cli("demo", "seed-data")
        assert result.returncode == 0
        assert "セットアップが完了" in result.stdout
