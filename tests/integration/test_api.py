"""Integration tests for REST API endpoints using FastAPI TestClient."""
from __future__ import annotations

import io
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.base import Base
import app.db.models  # noqa: F401
from app.db.session import get_db
from app.dependencies import get_file_store
from app.main import app
from app.storage.file_store import FileStore


SAMPLE_CSV = b"employee_id,name,department\nEMP001,Taro,Engineering\nEMP002,Hanako,Sales\n"
SAMPLE_CODE = b"import pandas as pd\nprint('analysis')\n"
SAMPLE_REPORT = b"# Report\n\nThis is the analysis report.\n"


@pytest.fixture()
def test_env(tmp_path: Path):
    """Set up test database, file store, and TestClient."""
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    Base.metadata.create_all(bind=engine)
    TestSession = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)

    file_store = FileStore(base_dir=tmp_path / "data_store")

    def override_get_db():
        session = TestSession()
        try:
            yield session
        finally:
            session.close()

    def override_get_file_store():
        return file_store

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_file_store] = override_get_file_store

    # Seed users
    session = TestSession()
    from app.services.auth_service import AuthService
    auth = AuthService(session)
    auth.create_user("hr_user", "HR User", "hr")
    auth.create_user("proposer_user", "Proposer", "proposer")
    session.close()

    client = TestClient(app, root_path="")

    yield {
        "client": client,
        "engine": engine,
        "session_factory": TestSession,
        "file_store": file_store,
        "tmp_path": tmp_path,
    }

    app.dependency_overrides.clear()
    engine.dispose()


def _login(client: TestClient, user_id: str):
    """Log in by setting session directly via the login endpoint."""
    resp = client.post("/login", data={"user_id": user_id}, follow_redirects=False)
    return resp


# ------------------------------------------------------------------
# Dataset API tests
# ------------------------------------------------------------------


class TestDatasetAPI:
    def test_create_dataset_requires_auth(self, test_env):
        client = test_env["client"]
        resp = client.post("/api/datasets", json={"name": "Test", "owner_user_id": "hr_user"})
        assert resp.status_code == 401

    def test_create_and_list_datasets(self, test_env):
        client = test_env["client"]
        _login(client, "hr_user")

        resp = client.post("/api/datasets", json={"name": "Test DS", "owner_user_id": "hr_user"})
        assert resp.status_code == 201
        data = resp.json()
        assert data["name"] == "Test DS"
        dataset_id = data["dataset_id"]

        resp = client.get("/api/datasets")
        assert resp.status_code == 200
        datasets = resp.json()["datasets"]
        assert len(datasets) >= 1
        assert any(d["dataset_id"] == dataset_id for d in datasets)

    def test_get_dataset(self, test_env):
        client = test_env["client"]
        _login(client, "hr_user")

        resp = client.post("/api/datasets", json={"name": "Get Test", "owner_user_id": "hr_user"})
        dataset_id = resp.json()["dataset_id"]

        resp = client.get(f"/api/datasets/{dataset_id}")
        assert resp.status_code == 200
        assert resp.json()["dataset_id"] == dataset_id

    def test_get_dataset_not_found(self, test_env):
        client = test_env["client"]
        _login(client, "hr_user")

        resp = client.get("/api/datasets/NONEXISTENT")
        assert resp.status_code == 404

    def test_upload_csv(self, test_env):
        client = test_env["client"]
        _login(client, "hr_user")

        resp = client.post("/api/datasets", json={"name": "Upload Test", "owner_user_id": "hr_user"})
        dataset_id = resp.json()["dataset_id"]

        resp = client.post(
            f"/api/datasets/{dataset_id}/upload",
            files={"file": ("employee_master.csv", io.BytesIO(SAMPLE_CSV), "text/csv")},
            data={"file_type": "employee_master"},
        )
        assert resp.status_code == 200
        assert len(resp.json()["files"]) == 1

    def test_publish_dataset(self, test_env):
        client = test_env["client"]
        _login(client, "hr_user")

        resp = client.post("/api/datasets", json={"name": "Pub Test", "owner_user_id": "hr_user"})
        dataset_id = resp.json()["dataset_id"]

        client.post(
            f"/api/datasets/{dataset_id}/upload",
            files={"file": ("employee_master.csv", io.BytesIO(SAMPLE_CSV), "text/csv")},
            data={"file_type": "employee_master"},
        )

        resp = client.post(f"/api/datasets/{dataset_id}/publish")
        assert resp.status_code == 200
        assert resp.json()["is_published"] is True

    def test_proposer_cannot_create_dataset(self, test_env):
        client = test_env["client"]
        _login(client, "proposer_user")

        resp = client.post("/api/datasets", json={"name": "Bad", "owner_user_id": "proposer_user"})
        assert resp.status_code == 403


# ------------------------------------------------------------------
# Proposal API tests
# ------------------------------------------------------------------


def _setup_published_dataset(client: TestClient) -> str:
    """Create and publish a dataset. Returns dataset_id."""
    _login(client, "hr_user")

    resp = client.post("/api/datasets", json={"name": "For Proposals", "owner_user_id": "hr_user"})
    dataset_id = resp.json()["dataset_id"]

    client.post(
        f"/api/datasets/{dataset_id}/upload",
        files={"file": ("employee_master.csv", io.BytesIO(SAMPLE_CSV), "text/csv")},
        data={"file_type": "employee_master"},
    )

    client.post(f"/api/datasets/{dataset_id}/publish")
    return dataset_id


class TestProposalAPI:
    def test_create_proposal(self, test_env):
        client = test_env["client"]
        dataset_id = _setup_published_dataset(client)

        _login(client, "proposer_user")
        resp = client.post(
            "/api/proposals",
            data={
                "dataset_id": dataset_id,
                "title": "Test Proposal",
                "summary": "A test",
            },
            files={
                "code_file": ("analysis.py", io.BytesIO(SAMPLE_CODE), "text/x-python"),
                "report_file": ("report.md", io.BytesIO(SAMPLE_REPORT), "text/markdown"),
            },
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data["title"] == "Test Proposal"
        assert data["status"] == "submitted"

    def test_list_proposals(self, test_env):
        client = test_env["client"]
        dataset_id = _setup_published_dataset(client)

        _login(client, "proposer_user")
        client.post(
            "/api/proposals",
            data={"dataset_id": dataset_id, "title": "P1", "summary": "S1"},
            files={
                "code_file": ("analysis.py", io.BytesIO(SAMPLE_CODE), "text/x-python"),
                "report_file": ("report.md", io.BytesIO(SAMPLE_REPORT), "text/markdown"),
            },
        )

        resp = client.get(f"/api/proposals?dataset_id={dataset_id}")
        assert resp.status_code == 200
        assert len(resp.json()["proposals"]) >= 1

    def test_get_proposal(self, test_env):
        client = test_env["client"]
        dataset_id = _setup_published_dataset(client)

        _login(client, "proposer_user")
        resp = client.post(
            "/api/proposals",
            data={"dataset_id": dataset_id, "title": "Get Test", "summary": "S"},
            files={
                "code_file": ("analysis.py", io.BytesIO(SAMPLE_CODE), "text/x-python"),
                "report_file": ("report.md", io.BytesIO(SAMPLE_REPORT), "text/markdown"),
            },
        )
        proposal_id = resp.json()["proposal_id"]

        resp = client.get(f"/api/proposals/{proposal_id}")
        assert resp.status_code == 200
        assert resp.json()["proposal_id"] == proposal_id

    def test_review_proposal_approve(self, test_env):
        client = test_env["client"]
        dataset_id = _setup_published_dataset(client)

        _login(client, "proposer_user")
        resp = client.post(
            "/api/proposals",
            data={"dataset_id": dataset_id, "title": "Review Test", "summary": "S"},
            files={
                "code_file": ("analysis.py", io.BytesIO(SAMPLE_CODE), "text/x-python"),
                "report_file": ("report.md", io.BytesIO(SAMPLE_REPORT), "text/markdown"),
            },
        )
        proposal_id = resp.json()["proposal_id"]

        _login(client, "hr_user")
        resp = client.post(
            f"/api/proposals/{proposal_id}/review",
            json={"action": "approve", "comment": "LGTM"},
        )
        assert resp.status_code == 200
        assert resp.json()["action"] == "approve"

        resp = client.get(f"/api/proposals/{proposal_id}")
        assert resp.json()["status"] == "approved"

    def test_review_proposal_reject(self, test_env):
        client = test_env["client"]
        dataset_id = _setup_published_dataset(client)

        _login(client, "proposer_user")
        resp = client.post(
            "/api/proposals",
            data={"dataset_id": dataset_id, "title": "Reject Test", "summary": "S"},
            files={
                "code_file": ("analysis.py", io.BytesIO(SAMPLE_CODE), "text/x-python"),
                "report_file": ("report.md", io.BytesIO(SAMPLE_REPORT), "text/markdown"),
            },
        )
        proposal_id = resp.json()["proposal_id"]

        _login(client, "hr_user")
        resp = client.post(
            f"/api/proposals/{proposal_id}/review",
            json={"action": "reject", "comment": "Needs work"},
        )
        assert resp.status_code == 200

        resp = client.get(f"/api/proposals/{proposal_id}")
        assert resp.json()["status"] == "rejected"

    def test_proposer_cannot_review(self, test_env):
        client = test_env["client"]
        dataset_id = _setup_published_dataset(client)

        _login(client, "proposer_user")
        resp = client.post(
            "/api/proposals",
            data={"dataset_id": dataset_id, "title": "P", "summary": "S"},
            files={
                "code_file": ("analysis.py", io.BytesIO(SAMPLE_CODE), "text/x-python"),
                "report_file": ("report.md", io.BytesIO(SAMPLE_REPORT), "text/markdown"),
            },
        )
        proposal_id = resp.json()["proposal_id"]

        resp = client.post(
            f"/api/proposals/{proposal_id}/review",
            json={"action": "approve", "comment": "hack"},
        )
        assert resp.status_code == 403

    def test_run_actual(self, test_env):
        client = test_env["client"]
        dataset_id = _setup_published_dataset(client)

        _login(client, "proposer_user")
        resp = client.post(
            "/api/proposals",
            data={"dataset_id": dataset_id, "title": "Run Test", "summary": "S"},
            files={
                "code_file": ("analysis.py", io.BytesIO(SAMPLE_CODE), "text/x-python"),
                "report_file": ("report.md", io.BytesIO(SAMPLE_REPORT), "text/markdown"),
            },
        )
        proposal_id = resp.json()["proposal_id"]

        _login(client, "hr_user")
        client.post(
            f"/api/proposals/{proposal_id}/review",
            json={"action": "approve", "comment": "OK"},
        )

        resp = client.post(f"/api/proposals/{proposal_id}/run_actual")
        assert resp.status_code == 200
        assert resp.json()["status"] == "executed_real"

    def test_run_actual_not_approved_fails(self, test_env):
        client = test_env["client"]
        dataset_id = _setup_published_dataset(client)

        _login(client, "proposer_user")
        resp = client.post(
            "/api/proposals",
            data={"dataset_id": dataset_id, "title": "Fail Run", "summary": "S"},
            files={
                "code_file": ("analysis.py", io.BytesIO(SAMPLE_CODE), "text/x-python"),
                "report_file": ("report.md", io.BytesIO(SAMPLE_REPORT), "text/markdown"),
            },
        )
        proposal_id = resp.json()["proposal_id"]

        _login(client, "hr_user")
        resp = client.post(f"/api/proposals/{proposal_id}/run_actual")
        assert resp.status_code == 400

    def test_get_review_comments(self, test_env):
        client = test_env["client"]
        dataset_id = _setup_published_dataset(client)

        _login(client, "proposer_user")
        resp = client.post(
            "/api/proposals",
            data={"dataset_id": dataset_id, "title": "Comments Test", "summary": "S"},
            files={
                "code_file": ("analysis.py", io.BytesIO(SAMPLE_CODE), "text/x-python"),
                "report_file": ("report.md", io.BytesIO(SAMPLE_REPORT), "text/markdown"),
            },
        )
        proposal_id = resp.json()["proposal_id"]

        _login(client, "hr_user")
        client.post(
            f"/api/proposals/{proposal_id}/review",
            json={"action": "comment", "comment": "Please fix X"},
        )
        client.post(
            f"/api/proposals/{proposal_id}/review",
            json={"action": "approve", "comment": "LGTM now"},
        )

        resp = client.get(f"/api/proposals/{proposal_id}/comments")
        assert resp.status_code == 200
        comments = resp.json()
        assert len(comments) == 2
