from __future__ import annotations

import json
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

from app.config import EXECUTION_TIMEOUT_SECONDS
from app.execution.output_validator import OutputValidator
from app.execution.package_validator import PackageValidator
from app.storage.file_store import FileStore


@dataclass
class ExecutionResult:
    success: bool
    stdout_path: Path | None
    stderr_path: Path | None
    output_path: Path | None
    output_data: dict | None
    errors: list[str] = field(default_factory=list)


class ExecutionRunner:
    def __init__(self, file_store: FileStore, timeout: int = EXECUTION_TIMEOUT_SECONDS) -> None:
        self.file_store = file_store
        self.timeout = timeout
        self._pkg_validator = PackageValidator()
        self._out_validator = OutputValidator()

    def run(self, extracted_dir: Path, data_dir: Path, execution_id: str) -> ExecutionResult:
        """Execute a submission package and return the result.

        Steps:
            1. Validate package structure (manifest.json, entry point).
            2. Read manifest.json to determine the entry point.
            3. Run ``python <entry_point>`` via subprocess with data paths as
               arguments and a configurable timeout.
            4. Persist stdout / stderr (and output.json if present) via
               FileStore.
            5. Validate output.json.
            6. Return an ExecutionResult summarising the outcome.
        """
        # 1. Validate package
        pkg_valid, pkg_errors = self._pkg_validator.validate(extracted_dir)
        if not pkg_valid:
            return ExecutionResult(
                success=False,
                stdout_path=None,
                stderr_path=None,
                output_path=None,
                output_data=None,
                errors=pkg_errors,
            )

        # 2. Read manifest
        manifest = json.loads((extracted_dir / "manifest.json").read_text(encoding="utf-8"))
        entry_point = manifest["entry_point"]

        # 3. Determine output path and run subprocess
        output_file = extracted_dir / "output.json"

        cmd = [
            sys.executable,
            entry_point,
            "--employee-master",
            str(data_dir / "employee_master.csv"),
            "--project-allocation",
            str(data_dir / "project_allocation.csv"),
            "--working-hours",
            str(data_dir / "working_hours.csv"),
            "--output",
            str(output_file),
        ]

        try:
            proc = subprocess.run(
                cmd,
                cwd=str(extracted_dir),
                capture_output=True,
                text=True,
                timeout=self.timeout,
            )
            stdout = proc.stdout
            stderr = proc.stderr
            timed_out = False
        except subprocess.TimeoutExpired as exc:
            stdout = exc.stdout or ""
            stderr = exc.stderr or ""
            if isinstance(stdout, bytes):
                stdout = stdout.decode("utf-8", errors="replace")
            if isinstance(stderr, bytes):
                stderr = stderr.decode("utf-8", errors="replace")
            timed_out = True

        # 4. Persist stdout / stderr / output.json via FileStore
        output_json_text: str | None = None
        if output_file.exists():
            output_json_text = output_file.read_text(encoding="utf-8")

        stdout_path, stderr_path, json_path = self.file_store.save_execution_output(
            execution_id=execution_id,
            stdout=stdout,
            stderr=stderr,
            output_json=output_json_text,
        )

        if timed_out:
            return ExecutionResult(
                success=False,
                stdout_path=stdout_path,
                stderr_path=stderr_path,
                output_path=json_path,
                output_data=None,
                errors=[f"Execution timed out after {self.timeout} seconds"],
            )

        if proc.returncode != 0:
            return ExecutionResult(
                success=False,
                stdout_path=stdout_path,
                stderr_path=stderr_path,
                output_path=json_path,
                output_data=None,
                errors=[f"Process exited with return code {proc.returncode}"],
            )

        # 5. Validate output.json
        if json_path is None:
            return ExecutionResult(
                success=False,
                stdout_path=stdout_path,
                stderr_path=stderr_path,
                output_path=None,
                output_data=None,
                errors=["Submission did not produce output.json"],
            )

        out_valid, output_data, out_errors = self._out_validator.validate(json_path)
        if not out_valid:
            return ExecutionResult(
                success=False,
                stdout_path=stdout_path,
                stderr_path=stderr_path,
                output_path=json_path,
                output_data=output_data,
                errors=out_errors,
            )

        # 6. Success
        return ExecutionResult(
            success=True,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            output_path=json_path,
            output_data=output_data,
            errors=[],
        )
