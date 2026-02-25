from __future__ import annotations

import json

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.db.models import (
    Dataset,
    Proposal,
    ProposalStatus,
    ReviewAction,
    ReviewComment,
    User,
    UserRole,
)
from app.storage.file_store import FileStore
from app.utils.ids import generate_id


class ProposalService:
    def __init__(self, db: Session, file_store: FileStore):
        self.db = db
        self.file_store = file_store

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def _get_user(self, user_id: str) -> User:
        user = self.db.execute(
            select(User).where(User.user_id == user_id)
        ).scalar_one_or_none()
        if user is None:
            raise ValueError(f"User not found: {user_id}")
        return user

    def _get_dataset(self, dataset_id: str) -> Dataset:
        ds = self.db.execute(
            select(Dataset).where(Dataset.dataset_id == dataset_id)
        ).scalar_one_or_none()
        if ds is None:
            raise ValueError(f"Dataset not found: {dataset_id}")
        return ds

    def _next_proposal_id(self) -> str:
        max_id = self.db.execute(
            select(func.count()).select_from(Proposal)
        ).scalar() or 0
        return generate_id("PRO", max_id + 1)

    # ------------------------------------------------------------------
    # public API
    # ------------------------------------------------------------------

    def create_proposal(
        self,
        actor_user_id: str,
        dataset_id: str,
        title: str,
        summary: str,
        code_content: bytes,
        report_content: bytes,
        execution_command: str | None = None,
        expected_outputs: list[str] | None = None,
    ) -> Proposal:
        user = self._get_user(actor_user_id)
        dataset = self._get_dataset(dataset_id)

        if not dataset.is_published:
            raise ValueError(f"Dataset {dataset_id} is not published yet")

        proposal_id = self._next_proposal_id()

        # Save files
        code_path = self.file_store.save_proposal_file(
            proposal_id, "analysis.py", code_content
        )
        report_path = self.file_store.save_proposal_file(
            proposal_id, "report.md", report_content
        )

        proposal = Proposal(
            proposal_id=proposal_id,
            dataset_id=dataset.id,
            user_id=user.id,
            title=title,
            summary=summary,
            code_path=str(code_path),
            report_path=str(report_path),
            execution_command=execution_command,
            expected_outputs=json.dumps(expected_outputs) if expected_outputs else None,
            status=ProposalStatus.submitted,
        )
        self.db.add(proposal)
        self.db.commit()
        self.db.refresh(proposal)
        return proposal

    def list_proposals(
        self, dataset_id: str, actor_user_id: str
    ) -> list[Proposal]:
        user = self._get_user(actor_user_id)
        dataset = self._get_dataset(dataset_id)

        stmt = select(Proposal).where(Proposal.dataset_id == dataset.id)

        # proposer can only see their own proposals
        if user.role == UserRole.proposer:
            stmt = stmt.where(Proposal.user_id == user.id)

        return list(self.db.execute(stmt).scalars().all())

    def get_proposal(
        self, proposal_id: str, actor_user_id: str
    ) -> Proposal:
        user = self._get_user(actor_user_id)

        proposal = self.db.execute(
            select(Proposal).where(Proposal.proposal_id == proposal_id)
        ).scalar_one_or_none()
        if proposal is None:
            raise ValueError(f"Proposal not found: {proposal_id}")

        # proposer can only see their own
        if user.role == UserRole.proposer and proposal.user_id != user.id:
            raise PermissionError("You can only view your own proposals")

        return proposal

    def review_proposal(
        self,
        proposal_id: str,
        reviewer_user_id: str,
        action: str,
        comment: str,
    ) -> ReviewComment:
        reviewer = self._get_user(reviewer_user_id)
        if reviewer.role != UserRole.hr:
            raise PermissionError("Only HR users can review proposals")

        proposal = self.db.execute(
            select(Proposal).where(Proposal.proposal_id == proposal_id)
        ).scalar_one_or_none()
        if proposal is None:
            raise ValueError(f"Proposal not found: {proposal_id}")

        review_action = ReviewAction(action)

        review_comment = ReviewComment(
            proposal_id=proposal.id,
            reviewer_user_id=reviewer.id,
            action=review_action,
            comment=comment,
        )
        self.db.add(review_comment)

        # Update proposal status based on action
        if review_action == ReviewAction.approve:
            proposal.status = ProposalStatus.approved
        elif review_action == ReviewAction.reject:
            proposal.status = ProposalStatus.rejected

        self.db.commit()
        self.db.refresh(review_comment)
        return review_comment

    def get_review_comments(
        self, proposal_id: str, actor_user_id: str
    ) -> list[ReviewComment]:
        user = self._get_user(actor_user_id)

        proposal = self.db.execute(
            select(Proposal).where(Proposal.proposal_id == proposal_id)
        ).scalar_one_or_none()
        if proposal is None:
            raise ValueError(f"Proposal not found: {proposal_id}")

        # proposer can only see comments on their own proposals
        if user.role == UserRole.proposer and proposal.user_id != user.id:
            raise PermissionError("You can only view comments on your own proposals")

        stmt = select(ReviewComment).where(
            ReviewComment.proposal_id == proposal.id
        ).order_by(ReviewComment.created_at)
        return list(self.db.execute(stmt).scalars().all())
