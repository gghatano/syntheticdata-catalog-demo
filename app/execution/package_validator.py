from __future__ import annotations

import json
from pathlib import Path


class PackageValidator:
    def validate(self, extracted_dir: Path) -> tuple[bool, list[str]]:
        """Validate an extracted submission directory.

        Checks that manifest.json exists and is valid JSON with the expected
        structure, and that the entry-point script (main.py) exists.

        Returns:
            (is_valid, errors)
        """
        errors: list[str] = []

        manifest_path = extracted_dir / "manifest.json"
        if not manifest_path.exists():
            errors.append("manifest.json not found in submission package")
            return False, errors

        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            errors.append(f"manifest.json is not valid JSON: {exc}")
            return False, errors

        if not isinstance(manifest, dict):
            errors.append("manifest.json must be a JSON object")
            return False, errors

        entry_point = manifest.get("entry_point")
        if not entry_point:
            errors.append("manifest.json missing 'entry_point' field")
            return False, errors

        entry_path = extracted_dir / entry_point
        if not entry_path.exists():
            errors.append(f"Entry point '{entry_point}' not found in submission package")
            return False, errors

        return len(errors) == 0, errors
