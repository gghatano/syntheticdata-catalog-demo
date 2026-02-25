from __future__ import annotations

import json

import pandas as pd
import pytest
from sqlalchemy.orm import Session

from app.catalog.catalog_generator import CatalogGenerator
from app.catalog.pii_detector import detect_pii, detect_pii_by_name, detect_pii_by_uniqueness
from app.catalog.stats_calculator import calculate_column_stats
from app.catalog.type_inferrer import infer_column_type, infer_types
from app.services.auth_service import AuthService
from app.services.catalog_service import CatalogService
from app.services.dataset_service import DatasetService
from app.storage.file_store import FileStore


# ---------- Type Inferrer ----------


class TestTypeInferrer:
    def test_infer_int_column(self):
        s = pd.Series([1, 2, 3, 4, 5])
        assert infer_column_type(s) == "int"

    def test_infer_float_column(self):
        s = pd.Series([1.1, 2.2, 3.3])
        assert infer_column_type(s) == "float"

    def test_infer_float_that_are_ints(self):
        s = pd.Series([1.0, 2.0, 3.0])
        assert infer_column_type(s) == "int"

    def test_infer_string_column(self):
        s = pd.Series(["a", "b", "c"])
        assert infer_column_type(s) == "string"

    def test_infer_date_column(self):
        s = pd.Series(["2024-01-01", "2024-02-01", "2024-03-01"])
        assert infer_column_type(s) == "date"

    def test_infer_bool_column(self):
        s = pd.Series([True, False, True])
        assert infer_column_type(s) == "bool"

    def test_infer_bool_string_column(self):
        s = pd.Series(["yes", "no", "yes"])
        assert infer_column_type(s) == "bool"

    def test_infer_empty_column(self):
        s = pd.Series([None, None, None])
        assert infer_column_type(s) == "string"

    def test_infer_mixed_with_nulls(self):
        s = pd.Series([1, 2, None, 4])
        result = infer_column_type(s)
        # int with nulls becomes float in pandas, but values are ints
        assert result == "int"

    def test_infer_types_full_df(self):
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "salary": [50000.5, 60000.3, 70000.1],
            "start_date": ["2020-01-01", "2021-06-15", "2022-12-01"],
        })
        result = infer_types(df)
        assert result["id"] == "int"
        assert result["name"] == "string"
        assert result["salary"] == "float"
        assert result["start_date"] == "date"


# ---------- PII Detector ----------


class TestPiiDetector:
    def test_detect_name_column(self):
        is_pii, reason = detect_pii_by_name("employee_name")
        assert is_pii is True
        assert reason is not None

    def test_detect_email_column(self):
        is_pii, reason = detect_pii_by_name("email")
        assert is_pii is True

    def test_detect_tel_column(self):
        is_pii, reason = detect_pii_by_name("tel")
        assert is_pii is True

    def test_detect_address_column(self):
        is_pii, reason = detect_pii_by_name("address")
        assert is_pii is True

    def test_detect_employee_id_column(self):
        is_pii, reason = detect_pii_by_name("employee_id")
        assert is_pii is True

    def test_non_pii_column(self):
        is_pii, reason = detect_pii_by_name("department")
        assert is_pii is False
        assert reason is None

    def test_detect_high_uniqueness(self):
        s = pd.Series([f"ID{i:04d}" for i in range(100)])
        is_pii, reason = detect_pii_by_uniqueness(s)
        assert is_pii is True
        assert "uniqueness" in reason.lower()

    def test_low_uniqueness_not_pii(self):
        s = pd.Series(["Engineering", "Sales", "Engineering", "HR", "Sales"] * 20)
        is_pii, reason = detect_pii_by_uniqueness(s)
        assert is_pii is False

    def test_numeric_column_skipped_for_uniqueness(self):
        s = pd.Series(range(100))
        is_pii, reason = detect_pii_by_uniqueness(s)
        assert is_pii is False

    def test_combined_detect_pii_by_name(self):
        s = pd.Series(["Alice", "Bob", "Charlie"])
        is_pii, reason = detect_pii("name", s)
        assert is_pii is True

    def test_combined_detect_pii_by_uniqueness(self):
        s = pd.Series([f"CUST{i:06d}" for i in range(100)])
        is_pii, reason = detect_pii("customer_code", s)
        assert is_pii is True


# ---------- Stats Calculator ----------


class TestStatsCalculator:
    def test_numeric_stats(self):
        s = pd.Series([10, 20, 30, 40, 50])
        stats = calculate_column_stats(s, "int")
        assert stats["missing_rate"] == 0.0
        assert stats["unique_count"] == 5
        assert stats["min"] == 10
        assert stats["max"] == 50
        assert stats["mean"] == 30.0

    def test_stats_with_missing(self):
        s = pd.Series([10, None, 30, None, 50])
        stats = calculate_column_stats(s, "float")
        assert stats["missing_rate"] == 0.4
        assert stats["missing_count"] == 2

    def test_string_stats(self):
        s = pd.Series(["a", "b", "a", "c"])
        stats = calculate_column_stats(s, "string")
        assert stats["unique_count"] == 3
        assert "mode" in stats

    def test_date_stats(self):
        s = pd.Series(["2024-01-01", "2024-06-15", "2024-12-31"])
        stats = calculate_column_stats(s, "date")
        assert stats["min"] == "2024-01-01"
        assert stats["max"] == "2024-12-31"

    def test_empty_column_stats(self):
        s = pd.Series([None, None], dtype=object)
        stats = calculate_column_stats(s, "string")
        assert stats["missing_rate"] == 1.0
        assert stats["unique_count"] == 0


# ---------- CatalogGenerator ----------


class TestCatalogGenerator:
    def test_generate_produces_all_columns(self):
        df = pd.DataFrame({
            "employee_id": ["EMP001", "EMP002", "EMP003"],
            "name": ["Taro", "Hanako", "Jiro"],
            "department": ["Eng", "Sales", "HR"],
            "salary": [500000, 600000, 700000],
        })
        gen = CatalogGenerator()
        columns = gen.generate(df)
        assert len(columns) == 4
        col_names = [c.column_name for c in columns]
        assert "employee_id" in col_names
        assert "name" in col_names

    def test_pii_detected_in_generated_catalog(self):
        df = pd.DataFrame({
            "employee_id": [f"EMP{i:03d}" for i in range(20)],
            "name": [f"Person{i}" for i in range(20)],
            "department": ["Eng", "Sales", "HR", "Finance"] * 5,
        })
        gen = CatalogGenerator()
        columns = gen.generate(df)
        pii_cols = {c.column_name for c in columns if c.is_pii}
        assert "name" in pii_cols
        assert "employee_id" in pii_cols
        assert "department" not in pii_cols

    def test_stats_json_is_valid(self):
        df = pd.DataFrame({"val": [1, 2, 3]})
        gen = CatalogGenerator()
        columns = gen.generate(df)
        stats = json.loads(columns[0].stats_json)
        assert "missing_rate" in stats
        assert "unique_count" in stats


# ---------- CatalogService ----------


def _make_sample_csv() -> bytes:
    """Generate a sample CSV with enough rows for uniqueness-based PII detection."""
    lines = ["employee_id,name,department,salary"]
    departments = ["Engineering", "Sales", "HR", "Finance"]
    for i in range(20):
        lines.append(f"EMP{i:03d},Person{i},{departments[i % 4]},{500000 + i * 10000}")
    return ("\n".join(lines) + "\n").encode()


SAMPLE_CSV = _make_sample_csv()


class TestCatalogServiceDerive:
    def test_derive_catalog_success(self, db_session: Session, file_store: FileStore):
        auth = AuthService(db_session)
        auth.create_user("hr_user", "HR User", "hr")

        ds_svc = DatasetService(db_session, file_store)
        ds = ds_svc.create_dataset("hr_user", "Test Dataset", {"employee_master": SAMPLE_CSV})

        cat_svc = CatalogService(db_session, file_store)
        columns = cat_svc.derive_catalog(ds.dataset_id, "hr_user")

        assert len(columns) == 4
        col_names = {c.column_name for c in columns}
        assert col_names == {"employee_id", "name", "department", "salary"}

    def test_derive_catalog_detects_pii(self, db_session: Session, file_store: FileStore):
        auth = AuthService(db_session)
        auth.create_user("hr_user", "HR User", "hr")

        ds_svc = DatasetService(db_session, file_store)
        ds = ds_svc.create_dataset("hr_user", "Test Dataset", {"employee_master": SAMPLE_CSV})

        cat_svc = CatalogService(db_session, file_store)
        columns = cat_svc.derive_catalog(ds.dataset_id, "hr_user")

        pii_map = {c.column_name: c.is_pii for c in columns}
        assert pii_map["name"] is True
        assert pii_map["employee_id"] is True
        assert pii_map["department"] is False

    def test_derive_catalog_non_hr_raises(self, db_session: Session, file_store: FileStore):
        auth = AuthService(db_session)
        auth.create_user("hr_user", "HR User", "hr")
        auth.create_user("proposer_user", "Proposer", "proposer")

        ds_svc = DatasetService(db_session, file_store)
        ds = ds_svc.create_dataset("hr_user", "Test Dataset", {"employee_master": SAMPLE_CSV})

        cat_svc = CatalogService(db_session, file_store)
        with pytest.raises(PermissionError):
            cat_svc.derive_catalog(ds.dataset_id, "proposer_user")

    def test_derive_catalog_replaces_existing(self, db_session: Session, file_store: FileStore):
        auth = AuthService(db_session)
        auth.create_user("hr_user", "HR User", "hr")

        ds_svc = DatasetService(db_session, file_store)
        ds = ds_svc.create_dataset("hr_user", "Test Dataset", {"employee_master": SAMPLE_CSV})

        cat_svc = CatalogService(db_session, file_store)
        cat_svc.derive_catalog(ds.dataset_id, "hr_user")
        columns2 = cat_svc.derive_catalog(ds.dataset_id, "hr_user")
        assert len(columns2) == 4


class TestCatalogServiceUpdate:
    def test_update_catalog_pii_flag(self, db_session: Session, file_store: FileStore):
        auth = AuthService(db_session)
        auth.create_user("hr_user", "HR User", "hr")

        ds_svc = DatasetService(db_session, file_store)
        ds = ds_svc.create_dataset("hr_user", "Test Dataset", {"employee_master": SAMPLE_CSV})

        cat_svc = CatalogService(db_session, file_store)
        cat_svc.derive_catalog(ds.dataset_id, "hr_user")

        updated = cat_svc.update_catalog(
            ds.dataset_id,
            [{"column_name": "department", "is_pii": True}],
            "hr_user",
        )
        dept_col = next(c for c in updated if c.column_name == "department")
        assert dept_col.is_pii is True

    def test_update_catalog_description(self, db_session: Session, file_store: FileStore):
        auth = AuthService(db_session)
        auth.create_user("hr_user", "HR User", "hr")

        ds_svc = DatasetService(db_session, file_store)
        ds = ds_svc.create_dataset("hr_user", "Test Dataset", {"employee_master": SAMPLE_CSV})

        cat_svc = CatalogService(db_session, file_store)
        cat_svc.derive_catalog(ds.dataset_id, "hr_user")

        updated = cat_svc.update_catalog(
            ds.dataset_id,
            [{"column_name": "salary", "description": "Annual salary in JPY"}],
            "hr_user",
        )
        salary_col = next(c for c in updated if c.column_name == "salary")
        assert salary_col.description == "Annual salary in JPY"


class TestCatalogServiceGet:
    def test_get_catalog_hr_access(self, db_session: Session, file_store: FileStore):
        auth = AuthService(db_session)
        auth.create_user("hr_user", "HR User", "hr")

        ds_svc = DatasetService(db_session, file_store)
        ds = ds_svc.create_dataset("hr_user", "Test Dataset", {"employee_master": SAMPLE_CSV})

        cat_svc = CatalogService(db_session, file_store)
        cat_svc.derive_catalog(ds.dataset_id, "hr_user")

        columns = cat_svc.get_catalog(ds.dataset_id, "hr_user")
        assert len(columns) == 4

    def test_get_catalog_proposer_unpublished_raises(self, db_session: Session, file_store: FileStore):
        auth = AuthService(db_session)
        auth.create_user("hr_user", "HR User", "hr")
        auth.create_user("proposer_user", "Proposer", "proposer")

        ds_svc = DatasetService(db_session, file_store)
        ds = ds_svc.create_dataset("hr_user", "Test Dataset", {"employee_master": SAMPLE_CSV})

        cat_svc = CatalogService(db_session, file_store)
        cat_svc.derive_catalog(ds.dataset_id, "hr_user")

        with pytest.raises(PermissionError):
            cat_svc.get_catalog(ds.dataset_id, "proposer_user")
