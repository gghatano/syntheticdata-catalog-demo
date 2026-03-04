"""Tests for Phase 08: Tasks 015-017 (multi-department, data requests, onboarding)."""
from __future__ import annotations

import pytest
from sqlalchemy.orm import Session

from app.db.models import Dataset, DataRequestStatus, Proposal, ProposalStatus, User, UserRole
from app.services.auth_service import AuthService
from app.services.data_request_service import DataRequestService
from app.services.dataset_service import DatasetService
from app.storage.file_store import FileStore


def _create_dataset(db: Session, owner: User, dataset_id: str = "DS0001") -> Dataset:
    ds = Dataset(dataset_id=dataset_id, name="Test DS", owner_user_id=owner.id, is_published=True)
    db.add(ds)
    db.commit()
    db.refresh(ds)
    return ds


def _create_proposal(
    db: Session, user: User, dataset: Dataset, proposal_id: str = "PROP0001", is_showcase: bool = False
) -> Proposal:
    p = Proposal(
        proposal_id=proposal_id,
        dataset_id=dataset.id,
        user_id=user.id,
        title="Test Proposal",
        summary="A showcase proposal",
        code_path="/tmp/code.py",
        report_path="/tmp/report.md",
        is_showcase=is_showcase,
        status=ProposalStatus.approved,
    )
    db.add(p)
    db.commit()
    db.refresh(p)
    return p


# ---------- Helpers ----------

def _create_user(db: Session, user_id: str, role: str, department: str | None = None) -> User:
    user = User(user_id=user_id, display_name=f"Test {user_id}", role=UserRole(role), department=department)
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


# ---------- Task 015: Multi-department ----------

class TestMultiDepartment:
    def test_data_owner_can_create_dataset(self, db_session: Session, file_store: FileStore):
        _create_user(db_session, "sales_owner", "data_owner", department="営業部")
        svc = DatasetService(db_session, file_store)
        ds = svc.create_dataset(
            owner_user_id="sales_owner",
            name="Sales Data",
            files={"employee_master": b"id,name\n1,Alice\n"},
        )
        assert ds.dataset_id == "DS0001"
        assert ds.name == "Sales Data"

    def test_proposer_cannot_create_dataset(self, db_session: Session, file_store: FileStore):
        _create_user(db_session, "proposer1", "proposer")
        svc = DatasetService(db_session, file_store)
        with pytest.raises(PermissionError):
            svc.create_dataset(
                owner_user_id="proposer1",
                name="Bad",
                files={"employee_master": b"id,name\n1,Alice\n"},
            )

    def test_hr_can_create_dataset(self, db_session: Session, file_store: FileStore):
        _create_user(db_session, "hr1", "hr")
        svc = DatasetService(db_session, file_store)
        ds = svc.create_dataset(
            owner_user_id="hr1",
            name="HR Data",
            files={"employee_master": b"id,name\n1,Bob\n"},
        )
        assert ds.dataset_id == "DS0001"

    def test_user_department_field(self, db_session: Session):
        user = _create_user(db_session, "dept_user", "data_owner", department="マーケティング部")
        assert user.department == "マーケティング部"

    def test_user_department_nullable(self, db_session: Session):
        user = _create_user(db_session, "no_dept", "hr")
        assert user.department is None

    def test_seed_users_includes_data_owners(self, db_session: Session):
        auth = AuthService(db_session)
        users = auth.seed_users()
        user_ids = [u.user_id for u in users]
        assert "sales_owner" in user_ids
        assert "mktg_owner" in user_ids
        sales = next(u for u in users if u.user_id == "sales_owner")
        assert sales.role == UserRole.data_owner


# ---------- Task 016: Data Request Board ----------

class TestDataRequestService:
    def test_create_request(self, db_session: Session):
        _create_user(db_session, "proposer1", "proposer")
        svc = DataRequestService(db_session)
        req = svc.create_request("proposer1", "Need sales data", "Monthly sales by region")
        assert req.request_id == "REQ0001"
        assert req.title == "Need sales data"
        assert req.status == DataRequestStatus.open
        assert req.vote_count == 0

    def test_create_request_with_columns(self, db_session: Session):
        _create_user(db_session, "proposer1", "proposer")
        svc = DataRequestService(db_session)
        req = svc.create_request("proposer1", "Title", "Desc", desired_columns="region, amount")
        assert req.desired_columns == "region, amount"

    def test_list_requests(self, db_session: Session):
        _create_user(db_session, "proposer1", "proposer")
        svc = DataRequestService(db_session)
        svc.create_request("proposer1", "Req A", "Desc A")
        svc.create_request("proposer1", "Req B", "Desc B")
        results = svc.list_requests()
        assert len(results) == 2

    def test_list_requests_with_filter(self, db_session: Session):
        _create_user(db_session, "proposer1", "proposer")
        _create_user(db_session, "hr1", "hr")
        svc = DataRequestService(db_session)
        req = svc.create_request("proposer1", "Req A", "Desc A")
        svc.create_request("proposer1", "Req B", "Desc B")
        svc.update_status(req.request_id, "hr1", "in_progress")
        results = svc.list_requests(status_filter="in_progress")
        assert len(results) == 1
        assert results[0].request_id == req.request_id

    def test_vote_adds_vote(self, db_session: Session):
        _create_user(db_session, "proposer1", "proposer")
        _create_user(db_session, "proposer2", "proposer")
        svc = DataRequestService(db_session)
        req = svc.create_request("proposer1", "Req", "Desc")
        req = svc.vote(req.request_id, "proposer2")
        assert req.vote_count == 1

    def test_vote_toggle_removes_vote(self, db_session: Session):
        _create_user(db_session, "proposer1", "proposer")
        _create_user(db_session, "proposer2", "proposer")
        svc = DataRequestService(db_session)
        req = svc.create_request("proposer1", "Req", "Desc")
        svc.vote(req.request_id, "proposer2")
        req = svc.vote(req.request_id, "proposer2")
        assert req.vote_count == 0

    def test_multiple_votes(self, db_session: Session):
        _create_user(db_session, "proposer1", "proposer")
        _create_user(db_session, "proposer2", "proposer")
        _create_user(db_session, "proposer3", "proposer")
        svc = DataRequestService(db_session)
        req = svc.create_request("proposer1", "Req", "Desc")
        svc.vote(req.request_id, "proposer2")
        req = svc.vote(req.request_id, "proposer3")
        assert req.vote_count == 2

    def test_update_status_hr(self, db_session: Session):
        _create_user(db_session, "proposer1", "proposer")
        _create_user(db_session, "hr1", "hr")
        svc = DataRequestService(db_session)
        req = svc.create_request("proposer1", "Req", "Desc")
        updated = svc.update_status(req.request_id, "hr1", "completed")
        assert updated.status == DataRequestStatus.completed

    def test_update_status_data_owner(self, db_session: Session):
        _create_user(db_session, "proposer1", "proposer")
        _create_user(db_session, "owner1", "data_owner")
        svc = DataRequestService(db_session)
        req = svc.create_request("proposer1", "Req", "Desc")
        updated = svc.update_status(req.request_id, "owner1", "in_progress")
        assert updated.status == DataRequestStatus.in_progress

    def test_update_status_proposer_denied(self, db_session: Session):
        _create_user(db_session, "proposer1", "proposer")
        svc = DataRequestService(db_session)
        req = svc.create_request("proposer1", "Req", "Desc")
        with pytest.raises(PermissionError):
            svc.update_status(req.request_id, "proposer1", "completed")

    def test_vote_nonexistent_request(self, db_session: Session):
        _create_user(db_session, "proposer1", "proposer")
        svc = DataRequestService(db_session)
        with pytest.raises(ValueError):
            svc.vote("REQ9999", "proposer1")


# ---------- Task: Data Request x Showcase Connection ----------


class TestDataRequestShowcase:
    def test_create_request_with_showcase(self, db_session: Session, file_store: FileStore):
        proposer = _create_user(db_session, "proposer1", "proposer")
        hr = _create_user(db_session, "hr1", "hr")
        ds = _create_dataset(db_session, hr)
        proposal = _create_proposal(db_session, proposer, ds, is_showcase=True)

        svc = DataRequestService(db_session)
        req = svc.create_request(
            "proposer1", "Need more data", "Like this analysis",
            showcase_proposal_id=proposal.id,
        )
        assert req.showcase_proposal_id == proposal.id
        assert req.showcase_proposal.proposal_id == "PROP0001"

    def test_create_request_without_showcase(self, db_session: Session):
        _create_user(db_session, "proposer1", "proposer")
        svc = DataRequestService(db_session)
        req = svc.create_request("proposer1", "Title", "Desc")
        assert req.showcase_proposal_id is None
        assert req.showcase_proposal is None

    def test_create_request_non_showcase_proposal_rejected(self, db_session: Session, file_store: FileStore):
        proposer = _create_user(db_session, "proposer1", "proposer")
        hr = _create_user(db_session, "hr1", "hr")
        ds = _create_dataset(db_session, hr)
        proposal = _create_proposal(db_session, proposer, ds, is_showcase=False)

        svc = DataRequestService(db_session)
        with pytest.raises(ValueError, match="ショーケース事例ではありません"):
            svc.create_request(
                "proposer1", "Title", "Desc",
                showcase_proposal_id=proposal.id,
            )

    def test_create_request_nonexistent_proposal_rejected(self, db_session: Session):
        _create_user(db_session, "proposer1", "proposer")
        svc = DataRequestService(db_session)
        with pytest.raises(ValueError, match="Proposal not found"):
            svc.create_request(
                "proposer1", "Title", "Desc",
                showcase_proposal_id=99999,
            )
