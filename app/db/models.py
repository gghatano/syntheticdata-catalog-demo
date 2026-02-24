import enum
from datetime import datetime

from sqlalchemy import Enum, ForeignKey, Integer, String, Text, Boolean
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


# ---------- Enums ----------

class UserRole(str, enum.Enum):
    hr = "hr"
    proposer = "proposer"
    admin = "admin"


class FileType(str, enum.Enum):
    employee_master = "employee_master"
    project_allocation = "project_allocation"
    working_hours = "working_hours"


class SubmissionStatus(str, enum.Enum):
    draft = "draft"
    submitted = "submitted"
    validation_failed = "validation_failed"
    under_review = "under_review"
    approved = "approved"
    rejected = "rejected"
    executed_synthetic = "executed_synthetic"
    executed_real = "executed_real"
    execution_failed = "execution_failed"


class ExecutionMode(str, enum.Enum):
    synthetic = "synthetic"
    real = "real"


class ExecutionStatus(str, enum.Enum):
    queued = "queued"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"
    timeout = "timeout"


class ResultScope(str, enum.Enum):
    private = "private"
    submitter = "submitter"
    public = "public"


# ---------- Models ----------

class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    display_name: Mapped[str] = mapped_column(String, nullable=False)
    role: Mapped[UserRole] = mapped_column(Enum(UserRole), nullable=False)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)

    datasets: Mapped[list["Dataset"]] = relationship(back_populates="owner")
    submissions: Mapped[list["Submission"]] = relationship(back_populates="user")


class Dataset(Base):
    __tablename__ = "datasets"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    dataset_id: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String, nullable=False)
    owner_user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), nullable=False)
    is_published: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, onupdate=datetime.utcnow)

    owner: Mapped["User"] = relationship(back_populates="datasets")
    files: Mapped[list["DatasetFile"]] = relationship(back_populates="dataset")
    artifacts: Mapped[list["SyntheticArtifact"]] = relationship(back_populates="dataset")
    submissions: Mapped[list["Submission"]] = relationship(back_populates="dataset")


class DatasetFile(Base):
    __tablename__ = "dataset_files"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    dataset_id: Mapped[int] = mapped_column(ForeignKey("datasets.id"), nullable=False)
    file_type: Mapped[FileType] = mapped_column(Enum(FileType), nullable=False)
    file_path: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)

    dataset: Mapped["Dataset"] = relationship(back_populates="files")


class SyntheticArtifact(Base):
    __tablename__ = "synthetic_artifacts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    dataset_id: Mapped[int] = mapped_column(ForeignKey("datasets.id"), nullable=False)
    seed: Mapped[int] = mapped_column(Integer, nullable=False)
    file_type: Mapped[FileType] = mapped_column(Enum(FileType), nullable=False)
    file_path: Mapped[str] = mapped_column(String, nullable=False)
    quality_report_path: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)

    dataset: Mapped["Dataset"] = relationship(back_populates="artifacts")


class Submission(Base):
    __tablename__ = "submissions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    submission_id: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    dataset_id: Mapped[int] = mapped_column(ForeignKey("datasets.id"), nullable=False)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), nullable=False)
    title: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    zip_path: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[SubmissionStatus] = mapped_column(
        Enum(SubmissionStatus), default=SubmissionStatus.draft, nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, onupdate=datetime.utcnow)

    dataset: Mapped["Dataset"] = relationship(back_populates="submissions")
    user: Mapped["User"] = relationship(back_populates="submissions")
    executions: Mapped[list["Execution"]] = relationship(back_populates="submission")


class Execution(Base):
    __tablename__ = "executions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    execution_id: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    submission_id: Mapped[int] = mapped_column(ForeignKey("submissions.id"), nullable=False)
    executor_user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), nullable=False)
    mode: Mapped[ExecutionMode] = mapped_column(Enum(ExecutionMode), nullable=False)
    status: Mapped[ExecutionStatus] = mapped_column(
        Enum(ExecutionStatus), default=ExecutionStatus.queued, nullable=False
    )
    stdout_path: Mapped[str | None] = mapped_column(String, nullable=True)
    stderr_path: Mapped[str | None] = mapped_column(String, nullable=True)
    output_path: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, onupdate=datetime.utcnow)

    submission: Mapped["Submission"] = relationship(back_populates="executions")
    executor: Mapped["User"] = relationship()
    results: Mapped[list["ExecutionResult"]] = relationship(back_populates="execution")


class ExecutionResult(Base):
    __tablename__ = "execution_results"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    execution_id: Mapped[int] = mapped_column(ForeignKey("executions.id"), nullable=False)
    result_json: Mapped[str] = mapped_column(Text, nullable=False)
    scope: Mapped[ResultScope] = mapped_column(Enum(ResultScope), nullable=False)
    published_at: Mapped[datetime | None] = mapped_column(nullable=True)

    execution: Mapped["Execution"] = relationship(back_populates="results")


class AuditLog(Base):
    __tablename__ = "audit_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[str] = mapped_column(String, nullable=False)
    action: Mapped[str] = mapped_column(String, nullable=False)
    target_type: Mapped[str] = mapped_column(String, nullable=False)
    target_id: Mapped[str] = mapped_column(String, nullable=False)
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)
