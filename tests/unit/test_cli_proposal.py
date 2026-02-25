from __future__ import annotations

import pytest
from sqlalchemy.orm import Session

from app.services.auth_service import AuthService
from app.services.dataset_service import DatasetService
from app.services.proposal_service import ProposalService
from app.services.synthetic_service import SyntheticService
from app.storage.file_store import FileStore


SAMPLE_CSV = b"employee_id,name,department,salary\nEMP001,Taro,Engineering,500000\nEMP002,Hanako,Sales,600000\nEMP003,Jiro,HR,700000\n"
SAMPLE_CODE = b"import pandas as pd\ndf = pd.read_csv('data.csv')\nprint(df.describe())\n"
SAMPLE_REPORT = b"# Analysis Report\n\nSummary of findings.\n"


def _setup_published_dataset(db_session: Session, file_store: FileStore):
    """Helper to create and publish a dataset for proposal tests."""
    auth = AuthService(db_session)
    auth.create_user("hr_user", "HR User", "hr")
    auth.create_user("prop_user", "Proposer", "proposer")

    ds_svc = DatasetService(db_session, file_store)
    ds = ds_svc.create_dataset("hr_user", "Test", {"employee_master": SAMPLE_CSV})

    syn_svc = SyntheticService(db_session, file_store)
    syn_svc.generate(ds.dataset_id, "hr_user", seed=42)
    syn_svc.publish(ds.dataset_id, "hr_user", True)

    return ds


class TestProposalCreateCLI:
    def test_create_proposal_success(self, db_session: Session, file_store: FileStore):
        ds = _setup_published_dataset(db_session, file_store)

        svc = ProposalService(db_session, file_store)
        proposal = svc.create_proposal(
            actor_user_id="prop_user",
            dataset_id=ds.dataset_id,
            title="Test Analysis",
            summary="Test summary",
            code_content=SAMPLE_CODE,
            report_content=SAMPLE_REPORT,
        )

        assert proposal.proposal_id == "PRO0001"
        assert proposal.title == "Test Analysis"
        assert proposal.status.value == "submitted"

    def test_create_proposal_unpublished_raises(self, db_session: Session, file_store: FileStore):
        auth = AuthService(db_session)
        auth.create_user("hr_user", "HR User", "hr")
        auth.create_user("prop_user", "Proposer", "proposer")

        ds_svc = DatasetService(db_session, file_store)
        ds = ds_svc.create_dataset("hr_user", "Test", {"employee_master": SAMPLE_CSV})

        svc = ProposalService(db_session, file_store)
        with pytest.raises(ValueError, match="not published"):
            svc.create_proposal(
                actor_user_id="prop_user",
                dataset_id=ds.dataset_id,
                title="Test",
                summary="Test",
                code_content=SAMPLE_CODE,
                report_content=SAMPLE_REPORT,
            )


class TestProposalListCLI:
    def test_list_proposals(self, db_session: Session, file_store: FileStore):
        ds = _setup_published_dataset(db_session, file_store)

        svc = ProposalService(db_session, file_store)
        svc.create_proposal("prop_user", ds.dataset_id, "Analysis 1", "Summary", SAMPLE_CODE, SAMPLE_REPORT)
        svc.create_proposal("prop_user", ds.dataset_id, "Analysis 2", "Summary", SAMPLE_CODE, SAMPLE_REPORT)

        proposals = svc.list_proposals(ds.dataset_id, "prop_user")
        assert len(proposals) == 2


class TestProposalReviewCLI:
    def test_review_approve(self, db_session: Session, file_store: FileStore):
        ds = _setup_published_dataset(db_session, file_store)

        svc = ProposalService(db_session, file_store)
        proposal = svc.create_proposal("prop_user", ds.dataset_id, "Test", "Summary", SAMPLE_CODE, SAMPLE_REPORT)

        review = svc.review_proposal(proposal.proposal_id, "hr_user", "approve", "Looks good!")
        assert review.action.value == "approve"

        updated = svc.get_proposal(proposal.proposal_id, "hr_user")
        assert updated.status.value == "approved"

    def test_review_reject(self, db_session: Session, file_store: FileStore):
        ds = _setup_published_dataset(db_session, file_store)

        svc = ProposalService(db_session, file_store)
        proposal = svc.create_proposal("prop_user", ds.dataset_id, "Test", "Summary", SAMPLE_CODE, SAMPLE_REPORT)

        review = svc.review_proposal(proposal.proposal_id, "hr_user", "reject", "Needs revision")
        assert review.action.value == "reject"

        updated = svc.get_proposal(proposal.proposal_id, "hr_user")
        assert updated.status.value == "rejected"

    def test_review_non_hr_raises(self, db_session: Session, file_store: FileStore):
        ds = _setup_published_dataset(db_session, file_store)

        svc = ProposalService(db_session, file_store)
        proposal = svc.create_proposal("prop_user", ds.dataset_id, "Test", "Summary", SAMPLE_CODE, SAMPLE_REPORT)

        with pytest.raises(PermissionError):
            svc.review_proposal(proposal.proposal_id, "prop_user", "approve", "OK")


class TestDatasetDownloadSyntheticCLI:
    def test_download_synthetic_success(self, db_session: Session, file_store: FileStore, tmp_path):
        ds = _setup_published_dataset(db_session, file_store)

        # Verify synthetic data exists
        syn_dir = file_store.get_synthetic_data_path(ds.dataset_id)
        csv_files = list(syn_dir.glob("*.csv"))
        assert len(csv_files) > 0

        # Simulate download (copy)
        import shutil
        output_dir = tmp_path / "download"
        output_dir.mkdir()
        for csv_file in csv_files:
            shutil.copy2(csv_file, output_dir / csv_file.name)

        copied_files = list(output_dir.glob("*.csv"))
        assert len(copied_files) == len(csv_files)
