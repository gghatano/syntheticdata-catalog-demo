"""Tests for Phase 06: Tasks 010-012."""
from __future__ import annotations

import json
import zipfile
from io import BytesIO

import pandas as pd
import pytest
from sqlalchemy.orm import Session

from app.db.models import CatalogColumn, Dataset, DatasetTag, FileType, SyntheticArtifact, User, UserRole
from app.services.dataset_service import DatasetService
from app.services.profiling_service import ProfilingService
from app.services.template_service import TemplateService
from app.storage.file_store import FileStore


def _create_user(db: Session, user_id: str, role: UserRole) -> User:
    user = User(user_id=user_id, display_name=user_id, role=role)
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def _create_dataset(db: Session, owner: User, dataset_id: str = "DS0001", published: bool = True) -> Dataset:
    ds = Dataset(
        dataset_id=dataset_id,
        name="Test Dataset",
        description="A test dataset for analysis",
        owner_user_id=owner.id,
        is_published=published,
    )
    db.add(ds)
    db.commit()
    db.refresh(ds)
    return ds


def _add_catalog_columns(db: Session, dataset: Dataset) -> list[CatalogColumn]:
    cols = []
    for name, dtype, stats in [
        ("age", "int64", json.dumps({"min": 20, "max": 65, "mean": 35, "count": 100})),
        ("department", "object", json.dumps({"value_counts": {"HR": 30, "Eng": 50, "Sales": 20}, "count": 100})),
    ]:
        col = CatalogColumn(
            dataset_id=dataset.id,
            column_name=name,
            inferred_type=dtype,
            description=f"{name} column",
            stats_json=stats,
        )
        db.add(col)
        cols.append(col)
    db.commit()
    for c in cols:
        db.refresh(c)
    return cols


def _create_synthetic_csv(db: Session, dataset: Dataset, file_store: FileStore) -> SyntheticArtifact:
    """Create a synthetic CSV artifact with test data."""
    df = pd.DataFrame({
        "age": [25, 30, 35, 40, 45, 50, 55, 60, 28, 33],
        "department": ["HR", "Eng", "Eng", "Sales", "HR", "Eng", "Sales", "HR", "Eng", "Sales"],
    })
    csv_path = file_store.save_synthetic_data(dataset.dataset_id, "employee_master", df)
    artifact = SyntheticArtifact(
        dataset_id=dataset.id,
        seed=42,
        file_type=FileType.employee_master,
        file_path=str(csv_path),
    )
    db.add(artifact)
    db.commit()
    db.refresh(artifact)
    return artifact


# ---------- Task 010: Template Service ----------


class TestTemplateService:
    def test_generate_template_zip(self, db_session: Session, file_store: FileStore):
        hr = _create_user(db_session, "hr1", UserRole.hr)
        ds = _create_dataset(db_session, hr)
        _add_catalog_columns(db_session, ds)

        svc = TemplateService(db_session, file_store)
        zip_bytes = svc.generate_template_zip("DS0001", "hr1")

        assert len(zip_bytes) > 0
        with zipfile.ZipFile(BytesIO(zip_bytes)) as zf:
            names = zf.namelist()
            assert "analysis.py" in names
            assert "report.md" in names
            assert "manifest.json" in names

            analysis = zf.read("analysis.py").decode("utf-8")
            assert "Test Dataset" in analysis
            assert "DS0001" in analysis

            manifest = json.loads(zf.read("manifest.json"))
            assert manifest["entry_point"] == "analysis.py"

    def test_generate_template_zip_includes_columns(self, db_session: Session, file_store: FileStore):
        hr = _create_user(db_session, "hr1", UserRole.hr)
        ds = _create_dataset(db_session, hr)
        _add_catalog_columns(db_session, ds)

        svc = TemplateService(db_session, file_store)
        zip_bytes = svc.generate_template_zip("DS0001", "hr1")

        with zipfile.ZipFile(BytesIO(zip_bytes)) as zf:
            report = zf.read("report.md").decode("utf-8")
            assert "age" in report
            assert "department" in report

    def test_generate_template_not_found(self, db_session: Session, file_store: FileStore):
        _create_user(db_session, "hr1", UserRole.hr)
        svc = TemplateService(db_session, file_store)
        with pytest.raises(ValueError, match="Dataset not found"):
            svc.generate_template_zip("DSXXXX", "hr1")


# ---------- Task 011: Search & Tags ----------


class TestDatasetSearch:
    def test_search_by_name(self, db_session: Session, file_store: FileStore):
        hr = _create_user(db_session, "hr1", UserRole.hr)
        _create_dataset(db_session, hr, "DS0001")
        ds2 = Dataset(
            dataset_id="DS0002", name="Employee Data", owner_user_id=hr.id, is_published=True
        )
        db_session.add(ds2)
        db_session.commit()

        svc = DatasetService(db_session, file_store)
        results = svc.search_datasets(query="Employee")
        assert len(results) == 1
        assert results[0].dataset_id == "DS0002"

    def test_search_by_description(self, db_session: Session, file_store: FileStore):
        hr = _create_user(db_session, "hr1", UserRole.hr)
        _create_dataset(db_session, hr, "DS0001")

        svc = DatasetService(db_session, file_store)
        results = svc.search_datasets(query="test dataset")
        assert len(results) == 1

    def test_search_only_published(self, db_session: Session, file_store: FileStore):
        hr = _create_user(db_session, "hr1", UserRole.hr)
        _create_dataset(db_session, hr, "DS0001", published=False)

        svc = DatasetService(db_session, file_store)
        results = svc.search_datasets()
        assert len(results) == 0

    def test_search_by_tag(self, db_session: Session, file_store: FileStore):
        hr = _create_user(db_session, "hr1", UserRole.hr)
        ds = _create_dataset(db_session, hr, "DS0001")
        tag = DatasetTag(dataset_id=ds.id, tag_name="hr-data")
        db_session.add(tag)
        db_session.commit()

        svc = DatasetService(db_session, file_store)
        results = svc.search_datasets(tags=["hr-data"])
        assert len(results) == 1

        results = svc.search_datasets(tags=["nonexistent"])
        assert len(results) == 0

    def test_sort_by_name(self, db_session: Session, file_store: FileStore):
        hr = _create_user(db_session, "hr1", UserRole.hr)
        ds1 = Dataset(dataset_id="DS0001", name="Zebra", owner_user_id=hr.id, is_published=True)
        ds2 = Dataset(dataset_id="DS0002", name="Alpha", owner_user_id=hr.id, is_published=True)
        db_session.add_all([ds1, ds2])
        db_session.commit()

        svc = DatasetService(db_session, file_store)
        results = svc.search_datasets(sort_by="name")
        assert results[0].name == "Alpha"
        assert results[1].name == "Zebra"


class TestTagCRUD:
    def test_add_and_remove_tag(self, db_session: Session, file_store: FileStore):
        hr = _create_user(db_session, "hr1", UserRole.hr)
        _create_dataset(db_session, hr, "DS0001")

        svc = DatasetService(db_session, file_store)
        tag = svc.add_tag("DS0001", "important", "hr1")
        assert tag.tag_name == "important"

        # Adding same tag again should be idempotent
        tag2 = svc.add_tag("DS0001", "important", "hr1")
        assert tag2.id == tag.id

        svc.remove_tag("DS0001", "important", "hr1")
        ds = svc.get_dataset("DS0001", "hr1")
        assert len(ds.tags) == 0

    def test_proposer_cannot_add_tag(self, db_session: Session, file_store: FileStore):
        hr = _create_user(db_session, "hr1", UserRole.hr)
        _create_user(db_session, "prop1", UserRole.proposer)
        _create_dataset(db_session, hr, "DS0001")

        svc = DatasetService(db_session, file_store)
        with pytest.raises(PermissionError):
            svc.add_tag("DS0001", "test", "prop1")


# ---------- Task 012: Profiling ----------


class TestProfilingService:
    def test_numeric_profile_fallback(self, db_session: Session):
        """Without CSV, falls back to stats_json."""
        hr = _create_user(db_session, "hr1", UserRole.hr)
        ds = _create_dataset(db_session, hr)
        _add_catalog_columns(db_session, ds)

        svc = ProfilingService(db_session)
        profiles = svc.get_profile_data("DS0001", "hr1")

        assert len(profiles) == 2

        age_profile = next(p for p in profiles if p["column_name"] == "age")
        assert age_profile["chart_type"] == "histogram"
        assert "labels" in age_profile["chart_data"]
        assert "values" in age_profile["chart_data"]

    def test_categorical_profile_fallback(self, db_session: Session):
        """Without CSV, falls back to stats_json value_counts."""
        hr = _create_user(db_session, "hr1", UserRole.hr)
        ds = _create_dataset(db_session, hr)
        _add_catalog_columns(db_session, ds)

        svc = ProfilingService(db_session)
        profiles = svc.get_profile_data("DS0001", "hr1")

        dept_profile = next(p for p in profiles if p["column_name"] == "department")
        assert dept_profile["chart_type"] == "bar"
        assert "HR" in dept_profile["chart_data"]["labels"]

    def test_numeric_profile_with_csv(self, db_session: Session, file_store: FileStore):
        """With synthetic CSV, generates histogram from actual data."""
        hr = _create_user(db_session, "hr1", UserRole.hr)
        ds = _create_dataset(db_session, hr)
        _add_catalog_columns(db_session, ds)
        _create_synthetic_csv(db_session, ds, file_store)

        svc = ProfilingService(db_session, file_store)
        profiles = svc.get_profile_data("DS0001", "hr1")

        age_profile = next(p for p in profiles if p["column_name"] == "age")
        assert age_profile["chart_type"] == "histogram"
        assert age_profile["total_count"] == 10
        assert age_profile["null_count"] == 0
        assert age_profile["unique_count"] == 10

        # Should have histogram bins
        chart = age_profile["chart_data"]
        assert len(chart["labels"]) == 10  # 10 bins
        assert sum(chart["values"]) == 10  # all 10 rows
        assert "min" in chart["stats"]
        assert "max" in chart["stats"]
        assert "median" in chart["stats"]
        assert "p25" in chart["stats"]
        assert "p75" in chart["stats"]
        assert chart["stats"]["min"] == 25.0
        assert chart["stats"]["max"] == 60.0

    def test_categorical_profile_with_csv(self, db_session: Session, file_store: FileStore):
        """With synthetic CSV, generates value_counts from actual data."""
        hr = _create_user(db_session, "hr1", UserRole.hr)
        ds = _create_dataset(db_session, hr)
        _add_catalog_columns(db_session, ds)
        _create_synthetic_csv(db_session, ds, file_store)

        svc = ProfilingService(db_session, file_store)
        profiles = svc.get_profile_data("DS0001", "hr1")

        dept_profile = next(p for p in profiles if p["column_name"] == "department")
        assert dept_profile["chart_type"] == "bar"
        assert dept_profile["total_count"] == 10
        assert dept_profile["unique_count"] == 3

        chart = dept_profile["chart_data"]
        assert "Eng" in chart["labels"]
        assert "HR" in chart["labels"]
        assert "Sales" in chart["labels"]
        assert chart["stats"]["cardinality"] == 3

    def test_profile_access_denied(self, db_session: Session):
        hr = _create_user(db_session, "hr1", UserRole.hr)
        _create_user(db_session, "prop1", UserRole.proposer)
        _create_dataset(db_session, hr, published=False)

        svc = ProfilingService(db_session)
        with pytest.raises(PermissionError):
            svc.get_profile_data("DS0001", "prop1")

    def test_profile_not_found(self, db_session: Session):
        _create_user(db_session, "hr1", UserRole.hr)
        svc = ProfilingService(db_session)
        with pytest.raises(ValueError, match="Dataset not found"):
            svc.get_profile_data("DSXXXX", "hr1")
