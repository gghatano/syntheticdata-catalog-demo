from __future__ import annotations

import pytest
from sqlalchemy.orm import Session

from app.db.models import ProposalStatus, ReviewAction
from app.services.auth_service import AuthService
from app.services.dataset_service import DatasetService
from app.services.proposal_service import ProposalService
from app.storage.file_store import FileStore


SAMPLE_CSV = b"employee_id,name,department\nEMP001,Taro,Engineering\n"
SAMPLE_CODE = b"import pandas as pd\nprint('analysis')\n"
SAMPLE_REPORT = b"# Report\n\nThis is the analysis report.\n"


def _setup_dataset(db_session: Session, file_store: FileStore) -> str:
    """Create an HR user, a dataset, and publish it. Returns dataset_id."""
    auth = AuthService(db_session)
    auth.create_user("hr_user", "HR User", "hr")

    ds_svc = DatasetService(db_session, file_store)
    ds = ds_svc.create_dataset("hr_user", "Test DS", {"employee_master": SAMPLE_CSV})
    # Manually publish the dataset
    ds.is_published = True
    db_session.commit()
    db_session.refresh(ds)
    return ds.dataset_id


def _setup_proposer(db_session: Session) -> str:
    """Create a proposer user. Returns user_id."""
    auth = AuthService(db_session)
    auth.create_user("proposer_user", "Proposer", "proposer")
    return "proposer_user"


# ---------- create_proposal ----------


class TestCreateProposal:
    def test_create_proposal_success(
        self, db_session: Session, file_store: FileStore
    ):
        dataset_id = _setup_dataset(db_session, file_store)
        _setup_proposer(db_session)

        svc = ProposalService(db_session, file_store)
        proposal = svc.create_proposal(
            actor_user_id="proposer_user",
            dataset_id=dataset_id,
            title="Test Proposal",
            summary="A test proposal summary",
            code_content=SAMPLE_CODE,
            report_content=SAMPLE_REPORT,
        )

        assert proposal.proposal_id == "PRO0001"
        assert proposal.title == "Test Proposal"
        assert proposal.status == ProposalStatus.submitted
        assert "analysis.py" in proposal.code_path
        assert "report.md" in proposal.report_path

    def test_create_proposal_unpublished_dataset_raises(
        self, db_session: Session, file_store: FileStore
    ):
        auth = AuthService(db_session)
        auth.create_user("hr_user", "HR User", "hr")
        auth.create_user("proposer_user", "Proposer", "proposer")

        ds_svc = DatasetService(db_session, file_store)
        ds = ds_svc.create_dataset("hr_user", "Unpub DS", {"employee_master": SAMPLE_CSV})

        svc = ProposalService(db_session, file_store)
        with pytest.raises(ValueError, match="not published"):
            svc.create_proposal(
                actor_user_id="proposer_user",
                dataset_id=ds.dataset_id,
                title="Bad",
                summary="Bad",
                code_content=SAMPLE_CODE,
                report_content=SAMPLE_REPORT,
            )

    def test_create_proposal_with_expected_outputs(
        self, db_session: Session, file_store: FileStore
    ):
        dataset_id = _setup_dataset(db_session, file_store)
        _setup_proposer(db_session)

        svc = ProposalService(db_session, file_store)
        proposal = svc.create_proposal(
            actor_user_id="proposer_user",
            dataset_id=dataset_id,
            title="With Outputs",
            summary="Summary",
            code_content=SAMPLE_CODE,
            report_content=SAMPLE_REPORT,
            execution_command="python analysis.py",
            expected_outputs=["result.csv"],
        )
        assert proposal.execution_command == "python analysis.py"
        assert '"result.csv"' in proposal.expected_outputs


# ---------- list_proposals / get_proposal ----------


class TestListAndGetProposal:
    def test_list_proposals_hr_sees_all(
        self, db_session: Session, file_store: FileStore
    ):
        dataset_id = _setup_dataset(db_session, file_store)
        _setup_proposer(db_session)

        auth = AuthService(db_session)
        auth.create_user("proposer2", "Proposer2", "proposer")

        svc = ProposalService(db_session, file_store)
        svc.create_proposal("proposer_user", dataset_id, "P1", "s1", SAMPLE_CODE, SAMPLE_REPORT)
        svc.create_proposal("proposer2", dataset_id, "P2", "s2", SAMPLE_CODE, SAMPLE_REPORT)

        proposals = svc.list_proposals(dataset_id, "hr_user")
        assert len(proposals) == 2

    def test_list_proposals_proposer_sees_own(
        self, db_session: Session, file_store: FileStore
    ):
        dataset_id = _setup_dataset(db_session, file_store)
        _setup_proposer(db_session)

        auth = AuthService(db_session)
        auth.create_user("proposer2", "Proposer2", "proposer")

        svc = ProposalService(db_session, file_store)
        svc.create_proposal("proposer_user", dataset_id, "P1", "s1", SAMPLE_CODE, SAMPLE_REPORT)
        svc.create_proposal("proposer2", dataset_id, "P2", "s2", SAMPLE_CODE, SAMPLE_REPORT)

        proposals = svc.list_proposals(dataset_id, "proposer_user")
        assert len(proposals) == 1
        assert proposals[0].title == "P1"

    def test_get_proposal_success(
        self, db_session: Session, file_store: FileStore
    ):
        dataset_id = _setup_dataset(db_session, file_store)
        _setup_proposer(db_session)

        svc = ProposalService(db_session, file_store)
        created = svc.create_proposal(
            "proposer_user", dataset_id, "P1", "s1", SAMPLE_CODE, SAMPLE_REPORT
        )

        fetched = svc.get_proposal(created.proposal_id, "proposer_user")
        assert fetched.proposal_id == created.proposal_id

    def test_get_proposal_other_proposer_raises(
        self, db_session: Session, file_store: FileStore
    ):
        dataset_id = _setup_dataset(db_session, file_store)
        _setup_proposer(db_session)

        auth = AuthService(db_session)
        auth.create_user("proposer2", "Proposer2", "proposer")

        svc = ProposalService(db_session, file_store)
        created = svc.create_proposal(
            "proposer_user", dataset_id, "P1", "s1", SAMPLE_CODE, SAMPLE_REPORT
        )

        with pytest.raises(PermissionError):
            svc.get_proposal(created.proposal_id, "proposer2")


# ---------- review_proposal ----------


class TestReviewProposal:
    def test_approve_proposal(
        self, db_session: Session, file_store: FileStore
    ):
        dataset_id = _setup_dataset(db_session, file_store)
        _setup_proposer(db_session)

        svc = ProposalService(db_session, file_store)
        proposal = svc.create_proposal(
            "proposer_user", dataset_id, "P1", "s1", SAMPLE_CODE, SAMPLE_REPORT
        )

        review = svc.review_proposal(
            proposal.proposal_id, "hr_user", "approve", "Looks good"
        )
        assert review.action == ReviewAction.approve

        updated = svc.get_proposal(proposal.proposal_id, "hr_user")
        assert updated.status == ProposalStatus.approved

    def test_reject_proposal(
        self, db_session: Session, file_store: FileStore
    ):
        dataset_id = _setup_dataset(db_session, file_store)
        _setup_proposer(db_session)

        svc = ProposalService(db_session, file_store)
        proposal = svc.create_proposal(
            "proposer_user", dataset_id, "P1", "s1", SAMPLE_CODE, SAMPLE_REPORT
        )

        svc.review_proposal(
            proposal.proposal_id, "hr_user", "reject", "Needs work"
        )

        updated = svc.get_proposal(proposal.proposal_id, "hr_user")
        assert updated.status == ProposalStatus.rejected

    def test_comment_does_not_change_status(
        self, db_session: Session, file_store: FileStore
    ):
        dataset_id = _setup_dataset(db_session, file_store)
        _setup_proposer(db_session)

        svc = ProposalService(db_session, file_store)
        proposal = svc.create_proposal(
            "proposer_user", dataset_id, "P1", "s1", SAMPLE_CODE, SAMPLE_REPORT
        )

        svc.review_proposal(
            proposal.proposal_id, "hr_user", "comment", "Please clarify"
        )

        updated = svc.get_proposal(proposal.proposal_id, "hr_user")
        assert updated.status == ProposalStatus.submitted

    def test_non_hr_cannot_review(
        self, db_session: Session, file_store: FileStore
    ):
        dataset_id = _setup_dataset(db_session, file_store)
        _setup_proposer(db_session)

        svc = ProposalService(db_session, file_store)
        proposal = svc.create_proposal(
            "proposer_user", dataset_id, "P1", "s1", SAMPLE_CODE, SAMPLE_REPORT
        )

        with pytest.raises(PermissionError, match="Only HR"):
            svc.review_proposal(
                proposal.proposal_id, "proposer_user", "approve", "Trying"
            )

    def test_get_review_comments(
        self, db_session: Session, file_store: FileStore
    ):
        dataset_id = _setup_dataset(db_session, file_store)
        _setup_proposer(db_session)

        svc = ProposalService(db_session, file_store)
        proposal = svc.create_proposal(
            "proposer_user", dataset_id, "P1", "s1", SAMPLE_CODE, SAMPLE_REPORT
        )

        svc.review_proposal(proposal.proposal_id, "hr_user", "comment", "Comment 1")
        svc.review_proposal(proposal.proposal_id, "hr_user", "approve", "LGTM")

        comments = svc.get_review_comments(proposal.proposal_id, "hr_user")
        assert len(comments) == 2
        assert comments[0].comment == "Comment 1"
        assert comments[1].action == ReviewAction.approve

    def test_proposer_can_see_own_comments(
        self, db_session: Session, file_store: FileStore
    ):
        dataset_id = _setup_dataset(db_session, file_store)
        _setup_proposer(db_session)

        svc = ProposalService(db_session, file_store)
        proposal = svc.create_proposal(
            "proposer_user", dataset_id, "P1", "s1", SAMPLE_CODE, SAMPLE_REPORT
        )

        svc.review_proposal(proposal.proposal_id, "hr_user", "comment", "Fix this")
        comments = svc.get_review_comments(proposal.proposal_id, "proposer_user")
        assert len(comments) == 1

    def test_other_proposer_cannot_see_comments(
        self, db_session: Session, file_store: FileStore
    ):
        dataset_id = _setup_dataset(db_session, file_store)
        _setup_proposer(db_session)

        auth = AuthService(db_session)
        auth.create_user("proposer2", "Proposer2", "proposer")

        svc = ProposalService(db_session, file_store)
        proposal = svc.create_proposal(
            "proposer_user", dataset_id, "P1", "s1", SAMPLE_CODE, SAMPLE_REPORT
        )

        svc.review_proposal(proposal.proposal_id, "hr_user", "comment", "Fix")

        with pytest.raises(PermissionError):
            svc.get_review_comments(proposal.proposal_id, "proposer2")
