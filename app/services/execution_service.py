from __future__ import annotations

import json

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.db.models import (
    Dataset,
    Execution,
    ExecutionMode,
    ExecutionResult,
    ExecutionStatus,
    ResultScope,
    Submission,
    SubmissionStatus,
    User,
    UserRole,
)
from app.execution.runner import ExecutionRunner
from app.execution.runner import ExecutionResult as RunnerResult
from app.storage.file_store import FileStore
from app.utils.ids import generate_id


class ExecutionService:
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

    def _next_execution_id(self) -> str:
        count = self.db.execute(
            select(func.count()).select_from(Execution)
        ).scalar() or 0
        return generate_id("EX", count + 1)

    # ------------------------------------------------------------------
    # public API
    # ------------------------------------------------------------------

    def run_submission(
        self, submission_id: str, executor_user_id: str, mode: str
    ) -> Execution:
        executor = self._get_user(executor_user_id)
        if executor.role != UserRole.hr:
            raise PermissionError("Only HR users can execute submissions")

        submission = self.db.execute(
            select(Submission).where(Submission.submission_id == submission_id)
        ).scalar_one_or_none()
        if submission is None:
            raise ValueError(f"Submission not found: {submission_id}")

        if submission.status != SubmissionStatus.approved:
            raise ValueError(
                f"Cannot execute submission in status '{submission.status.value}'. "
                "Only 'approved' submissions can be executed."
            )

        exec_mode = ExecutionMode(mode)
        execution_id = self._next_execution_id()

        execution = Execution(
            execution_id=execution_id,
            submission_id=submission.id,
            executor_user_id=executor.id,
            mode=exec_mode,
            status=ExecutionStatus.running,
        )
        self.db.add(execution)
        self.db.flush()

        # Determine data directory
        dataset = self.db.execute(
            select(Dataset).where(Dataset.id == submission.dataset_id)
        ).scalar_one()

        if exec_mode == ExecutionMode.synthetic:
            data_dir = self.file_store.get_synthetic_data_path(dataset.dataset_id)
        else:
            data_dir = self.file_store.get_real_data_path(dataset.dataset_id)

        # Determine extracted dir from zip_path
        from pathlib import Path

        zip_path = Path(submission.zip_path)
        extracted_dir = zip_path.parent / "extracted"

        # Run execution
        runner = ExecutionRunner(self.file_store)
        result: RunnerResult = runner.run(extracted_dir, data_dir, execution_id)

        # Update execution based on result
        if result.success:
            execution.status = ExecutionStatus.succeeded
        elif result.errors and "timed out" in result.errors[0].lower():
            execution.status = ExecutionStatus.timeout
        else:
            execution.status = ExecutionStatus.failed

        execution.stdout_path = str(result.stdout_path) if result.stdout_path else None
        execution.stderr_path = str(result.stderr_path) if result.stderr_path else None
        execution.output_path = str(result.output_path) if result.output_path else None

        # Create ExecutionResult record (scope=private)
        result_json = json.dumps(result.output_data) if result.output_data else json.dumps({"errors": result.errors})
        exec_result = ExecutionResult(
            execution_id=execution.id,
            result_json=result_json,
            scope=ResultScope.private,
        )
        self.db.add(exec_result)

        # Update submission status
        if result.success:
            if exec_mode == ExecutionMode.synthetic:
                submission.status = SubmissionStatus.executed_synthetic
            else:
                submission.status = SubmissionStatus.executed_real
        else:
            submission.status = SubmissionStatus.execution_failed

        self.db.commit()
        self.db.refresh(execution)
        return execution

    def get_execution(self, execution_id: str, actor_user_id: str) -> Execution:
        self._get_user(actor_user_id)

        execution = self.db.execute(
            select(Execution).where(Execution.execution_id == execution_id)
        ).scalar_one_or_none()
        if execution is None:
            raise ValueError(f"Execution not found: {execution_id}")

        return execution

    def list_executions(
        self, submission_id: str, actor_user_id: str
    ) -> list[Execution]:
        self._get_user(actor_user_id)

        submission = self.db.execute(
            select(Submission).where(Submission.submission_id == submission_id)
        ).scalar_one_or_none()
        if submission is None:
            raise ValueError(f"Submission not found: {submission_id}")

        return list(
            self.db.execute(
                select(Execution).where(Execution.submission_id == submission.id)
            ).scalars().all()
        )
