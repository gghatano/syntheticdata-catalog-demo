from __future__ import annotations

import json
from pathlib import Path

REQUIRED_KEYS = {"analysis_name", "results", "summary"}


class OutputValidator:
    def validate(self, output_path: Path) -> tuple[bool, dict | None, list[str]]:
        """Validate output.json produced by a submission.

        Checks that the file exists, is valid JSON, and contains the required
        top-level keys (analysis_name, results, summary).

        Returns:
            (is_valid, parsed_data, errors)
        """
        errors: list[str] = []

        if not output_path.exists():
            errors.append("output.json not found")
            return False, None, errors

        raw = output_path.read_text(encoding="utf-8")
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            errors.append(f"output.json is not valid JSON: {exc}")
            return False, None, errors

        if not isinstance(data, dict):
            errors.append("output.json must be a JSON object")
            return False, None, errors

        missing = REQUIRED_KEYS - data.keys()
        if missing:
            errors.append(f"output.json missing required keys: {sorted(missing)}")
            return False, data, errors

        return True, data, errors
