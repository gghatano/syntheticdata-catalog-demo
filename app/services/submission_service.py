from __future__ import annotations

import json

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.db.models import (
    Dataset,
    Submission,
    SubmissionStatus,
    User,
    UserRole,
)
from app.execution.package_validator import PackageValidator
from app.storage.file_store import FileStore
from app.utils.ids import generate_id


class SubmissionService:
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

    def _next_submission_id(self) -> str:
        max_id = self.db.execute(
            select(func.count()).select_from(Submission)
        ).scalar() or 0
        return generate_id("SUB", max_id + 1)

    # ------------------------------------------------------------------
    # public API
    # ------------------------------------------------------------------

    def create_submission(
        self,
        actor_user_id: str,
        dataset_id: str,
        title: str,
        description: str,
        zip_content: bytes,
    ) -> Submission:
        user = self._get_user(actor_user_id)
        dataset = self._get_dataset(dataset_id)

        if not dataset.is_published:
            raise ValueError(f"Dataset {dataset_id} is not published yet")

        submission_id = self._next_submission_id()

        # Save ZIP
        zip_path = self.file_store.save_submission_zip(submission_id, zip_content)

        # Extract ZIP
        extracted_dir = self.file_store.extract_submission(zip_path, submission_id)

        # Validate package
        validator = PackageValidator()
        is_valid, errors = validator.validate(extracted_dir)

        status = SubmissionStatus.submitted if is_valid else SubmissionStatus.validation_failed

        submission = Submission(
            submission_id=submission_id,
            dataset_id=dataset.id,
            user_id=user.id,
            title=title,
            description=description if is_valid else f"{description}\n\nValidation errors: {json.dumps(errors)}",
            zip_path=str(zip_path),
            status=status,
        )
        self.db.add(submission)
        self.db.commit()
        self.db.refresh(submission)
        return submission

    def list_submissions(self, dataset_id: str, actor_user_id: str) -> list[Submission]:
        user = self._get_user(actor_user_id)
        dataset = self._get_dataset(dataset_id)

        stmt = select(Submission).where(Submission.dataset_id == dataset.id)

        # proposer can only see their own submissions
        if user.role == UserRole.proposer:
            stmt = stmt.where(Submission.user_id == user.id)

        return list(self.db.execute(stmt).scalars().all())

    def get_submission(self, submission_id: str, actor_user_id: str) -> Submission:
        user = self._get_user(actor_user_id)

        submission = self.db.execute(
            select(Submission).where(Submission.submission_id == submission_id)
        ).scalar_one_or_none()
        if submission is None:
            raise ValueError(f"Submission not found: {submission_id}")

        # proposer can only see their own
        if user.role == UserRole.proposer and submission.user_id != user.id:
            raise PermissionError("You can only view your own submissions")

        return submission

    def approve_submission(self, submission_id: str, approver_user_id: str) -> Submission:
        user = self._get_user(approver_user_id)
        if user.role != UserRole.hr:
            raise PermissionError("Only HR users can approve submissions")

        submission = self.db.execute(
            select(Submission).where(Submission.submission_id == submission_id)
        ).scalar_one_or_none()
        if submission is None:
            raise ValueError(f"Submission not found: {submission_id}")

        if submission.status != SubmissionStatus.submitted:
            raise ValueError(
                f"Cannot approve submission in status '{submission.status.value}'. "
                "Only 'submitted' submissions can be approved."
            )

        submission.status = SubmissionStatus.approved
        self.db.commit()
        self.db.refresh(submission)
        return submission

    def reject_submission(
        self, submission_id: str, approver_user_id: str, reason: str
    ) -> Submission:
        user = self._get_user(approver_user_id)
        if user.role != UserRole.hr:
            raise PermissionError("Only HR users can reject submissions")

        submission = self.db.execute(
            select(Submission).where(Submission.submission_id == submission_id)
        ).scalar_one_or_none()
        if submission is None:
            raise ValueError(f"Submission not found: {submission_id}")

        if submission.status != SubmissionStatus.submitted:
            raise ValueError(
                f"Cannot reject submission in status '{submission.status.value}'. "
                "Only 'submitted' submissions can be rejected."
            )

        submission.status = SubmissionStatus.rejected
        submission.description = f"{submission.description}\n\nRejection reason: {reason}"
        self.db.commit()
        self.db.refresh(submission)
        return submission
