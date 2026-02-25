from __future__ import annotations

import json

import pytest
from sqlalchemy.orm import Session

from app.services.auth_service import AuthService
from app.services.catalog_service import CatalogService
from app.services.dataset_service import DatasetService
from app.storage.file_store import FileStore


SAMPLE_CSV = b"employee_id,name,department,salary\nEMP001,Taro,Engineering,500000\nEMP002,Hanako,Sales,600000\nEMP003,Jiro,HR,700000\n"


class TestCatalogDeriveCLI:
    """Test CatalogService.derive_catalog (used by CLI catalog derive)."""

    def test_derive_creates_catalog(self, db_session: Session, file_store: FileStore):
        auth = AuthService(db_session)
        auth.create_user("hr_user", "HR User", "hr")

        ds_svc = DatasetService(db_session, file_store)
        ds = ds_svc.create_dataset("hr_user", "Test", {"employee_master": SAMPLE_CSV})

        cat_svc = CatalogService(db_session, file_store)
        columns = cat_svc.derive_catalog(ds.dataset_id, "hr_user")

        assert len(columns) == 4
        col_names = {c.column_name for c in columns}
        assert "employee_id" in col_names
        assert "salary" in col_names

    def test_derive_non_hr_raises(self, db_session: Session, file_store: FileStore):
        auth = AuthService(db_session)
        auth.create_user("hr_user", "HR User", "hr")
        auth.create_user("prop_user", "Proposer", "proposer")

        ds_svc = DatasetService(db_session, file_store)
        ds = ds_svc.create_dataset("hr_user", "Test", {"employee_master": SAMPLE_CSV})

        cat_svc = CatalogService(db_session, file_store)
        with pytest.raises(PermissionError):
            cat_svc.derive_catalog(ds.dataset_id, "prop_user")


class TestCatalogShowCLI:
    """Test CatalogService.get_catalog (used by CLI catalog show)."""

    def test_show_catalog_after_derive(self, db_session: Session, file_store: FileStore):
        auth = AuthService(db_session)
        auth.create_user("hr_user", "HR User", "hr")

        ds_svc = DatasetService(db_session, file_store)
        ds = ds_svc.create_dataset("hr_user", "Test", {"employee_master": SAMPLE_CSV})

        cat_svc = CatalogService(db_session, file_store)
        cat_svc.derive_catalog(ds.dataset_id, "hr_user")
        columns = cat_svc.get_catalog(ds.dataset_id, "hr_user")

        assert len(columns) == 4
        for c in columns:
            stats = json.loads(c.stats_json)
            assert "missing_rate" in stats


class TestCatalogUpdateCLI:
    """Test CatalogService.update_catalog (used by CLI catalog update)."""

    def test_update_pii_flag(self, db_session: Session, file_store: FileStore):
        auth = AuthService(db_session)
        auth.create_user("hr_user", "HR User", "hr")

        ds_svc = DatasetService(db_session, file_store)
        ds = ds_svc.create_dataset("hr_user", "Test", {"employee_master": SAMPLE_CSV})

        cat_svc = CatalogService(db_session, file_store)
        cat_svc.derive_catalog(ds.dataset_id, "hr_user")
        columns = cat_svc.update_catalog(
            ds.dataset_id,
            [{"column_name": "department", "is_pii": True}],
            "hr_user",
        )
        dept = next(c for c in columns if c.column_name == "department")
        assert dept.is_pii is True

    def test_update_description(self, db_session: Session, file_store: FileStore):
        auth = AuthService(db_session)
        auth.create_user("hr_user", "HR User", "hr")

        ds_svc = DatasetService(db_session, file_store)
        ds = ds_svc.create_dataset("hr_user", "Test", {"employee_master": SAMPLE_CSV})

        cat_svc = CatalogService(db_session, file_store)
        cat_svc.derive_catalog(ds.dataset_id, "hr_user")
        columns = cat_svc.update_catalog(
            ds.dataset_id,
            [{"column_name": "salary", "description": "年収（円）"}],
            "hr_user",
        )
        salary = next(c for c in columns if c.column_name == "salary")
        assert salary.description == "年収（円）"
