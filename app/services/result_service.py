from __future__ import annotations

from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models import (
    Execution,
    ExecutionResult,
    ResultScope,
    Submission,
    User,
    UserRole,
)


class ResultService:
    def __init__(self, db: Session):
        self.db = db

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

    # ------------------------------------------------------------------
    # public API
    # ------------------------------------------------------------------

    def get_result(
        self, execution_id: str, actor_user_id: str
    ) -> ExecutionResult | None:
        user = self._get_user(actor_user_id)

        execution = self.db.execute(
            select(Execution).where(Execution.execution_id == execution_id)
        ).scalar_one_or_none()
        if execution is None:
            raise ValueError(f"Execution not found: {execution_id}")

        result = self.db.execute(
            select(ExecutionResult).where(ExecutionResult.execution_id == execution.id)
        ).scalar_one_or_none()
        if result is None:
            return None

        # HR can always see results
        if user.role == UserRole.hr:
            return result

        # Non-HR: check scope and ownership
        submission = self.db.execute(
            select(Submission).where(Submission.id == execution.submission_id)
        ).scalar_one()

        is_submitter = submission.user_id == user.id

        if result.scope == ResultScope.public:
            return result
        if result.scope == ResultScope.submitter and is_submitter:
            return result

        # private scope or non-submitter trying to access submitter scope
        raise PermissionError("You do not have permission to view this result")

    def publish_result(
        self, execution_id: str, actor_user_id: str, scope: str
    ) -> ExecutionResult:
        user = self._get_user(actor_user_id)
        if user.role != UserRole.hr:
            raise PermissionError("Only HR users can publish results")

        result_scope = ResultScope(scope)
        if result_scope == ResultScope.private:
            raise ValueError("Use 'submitter' or 'public' to publish results")

        execution = self.db.execute(
            select(Execution).where(Execution.execution_id == execution_id)
        ).scalar_one_or_none()
        if execution is None:
            raise ValueError(f"Execution not found: {execution_id}")

        result = self.db.execute(
            select(ExecutionResult).where(ExecutionResult.execution_id == execution.id)
        ).scalar_one_or_none()
        if result is None:
            raise ValueError(f"No result found for execution: {execution_id}")

        result.scope = result_scope
        result.published_at = datetime.utcnow()
        self.db.commit()
        self.db.refresh(result)
        return result
