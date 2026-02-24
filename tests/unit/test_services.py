from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy.orm import Session

from app.services.auth_service import AuthService
from app.services.dataset_service import DatasetService
from app.storage.file_store import FileStore
from app.synthetic.generator import SyntheticGenerator


# ---------- AuthService ----------


class TestAuthServiceCreateUser:
    def test_create_user_success(self, db_session: Session):
        svc = AuthService(db_session)
        user = svc.create_user("test_user", "Test User", "proposer")
        assert user.user_id == "test_user"
        assert user.display_name == "Test User"
        assert user.role.value == "proposer"

    def test_create_user_duplicate_raises(self, db_session: Session):
        svc = AuthService(db_session)
        svc.create_user("dup_user", "Dup", "hr")
        with pytest.raises(ValueError, match="already exists"):
            svc.create_user("dup_user", "Dup2", "hr")


class TestAuthServiceSeedUsers:
    def test_seed_users_creates_four(self, db_session: Session):
        svc = AuthService(db_session)
        users = svc.seed_users()
        assert len(users) == 4
        user_ids = {u.user_id for u in users}
        assert "hr_demo" in user_ids
        assert "admin_demo" in user_ids

    def test_seed_users_idempotent(self, db_session: Session):
        svc = AuthService(db_session)
        svc.seed_users()
        users2 = svc.seed_users()
        assert len(users2) == 4


# ---------- DatasetService ----------


SAMPLE_CSV = b"employee_id,name,department\nEMP001,Taro,Engineering\nEMP002,Hanako,Sales\n"


class TestDatasetServiceCreateDataset:
    def test_create_dataset_success(self, db_session: Session, file_store: FileStore):
        auth = AuthService(db_session)
        auth.create_user("hr_user", "HR User", "hr")

        svc = DatasetService(db_session, file_store)
        ds = svc.create_dataset(
            "hr_user",
            "Test Dataset",
            {"employee_master": SAMPLE_CSV},
        )
        assert ds.dataset_id == "DS0001"
        assert ds.name == "Test Dataset"
        assert len(ds.files) == 1

    def test_create_dataset_non_hr_raises(self, db_session: Session, file_store: FileStore):
        auth = AuthService(db_session)
        auth.create_user("proposer_user", "Proposer", "proposer")

        svc = DatasetService(db_session, file_store)
        with pytest.raises(PermissionError):
            svc.create_dataset("proposer_user", "Bad", {"employee_master": SAMPLE_CSV})


# ---------- SyntheticGenerator ----------


class TestSyntheticGenerator:
    def test_generate_preserves_shape(self):
        df = pd.DataFrame({
            "employee_id": ["EMP001", "EMP002", "EMP003"],
            "name": ["Taro", "Hanako", "Jiro"],
            "salary": [500, 600, 700],
        })
        gen = SyntheticGenerator()
        syn = gen.generate(df, seed=42)
        assert syn.shape == df.shape
        assert list(syn.columns) == list(df.columns)

    def test_generate_all_from_dir(self, tmp_path: Path):
        data_dir = tmp_path / "real" / "DS0001"
        data_dir.mkdir(parents=True)

        em = pd.DataFrame({
            "employee_id": ["EMP001", "EMP002"],
            "name": ["Taro", "Hanako"],
            "department": ["Eng", "Sales"],
        })
        em.to_csv(data_dir / "employee_master.csv", index=False)

        gen = SyntheticGenerator()
        results = gen.generate_all(data_dir, seed=42)
        assert "employee_master" in results
        assert len(results["employee_master"]) == 2
