from datetime import datetime

from pydantic import BaseModel, ConfigDict

from app.db.models import (
    ExecutionMode,
    ExecutionStatus,
    FileType,
    ProposalStatus,
    ResultScope,
    ReviewAction,
    SubmissionStatus,
    UserRole,
)


# ---------- User ----------


class UserCreate(BaseModel):
    user_id: str
    display_name: str
    role: UserRole


class UserResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    user_id: str
    display_name: str
    role: UserRole
    created_at: datetime


# ---------- Dataset ----------


class DatasetFileResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    file_type: FileType
    file_path: str


class DatasetCreate(BaseModel):
    name: str
    owner_user_id: str


class DatasetResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    dataset_id: str
    name: str
    owner_user_id: int
    is_published: bool
    created_at: datetime
    files: list[DatasetFileResponse] = []


class DatasetListResponse(BaseModel):
    datasets: list[DatasetResponse]


# ---------- Synthetic ----------


class SyntheticGenerateRequest(BaseModel):
    dataset_id: str
    seed: int = 42


class SyntheticArtifactResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    dataset_id: str
    file_type: FileType
    file_path: str
    seed: int
    quality_report_path: str | None = None


class QualityReportResponse(BaseModel):
    dataset_id: str
    report: dict


# ---------- Submission ----------


class SubmissionCreate(BaseModel):
    dataset_id: str
    title: str
    description: str


class SubmissionResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    submission_id: str
    dataset_id: str
    user_id: str
    title: str
    description: str
    status: SubmissionStatus
    created_at: datetime


class SubmissionListResponse(BaseModel):
    submissions: list[SubmissionResponse]


# ---------- Execution ----------


class ExecutionRunRequest(BaseModel):
    submission_id: str
    mode: ExecutionMode
    executor: str


class ExecutionResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    execution_id: str
    submission_id: str
    executor_user_id: str
    mode: ExecutionMode
    status: ExecutionStatus
    created_at: datetime


class ExecutionResultResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    execution_id: str
    result_json: dict
    scope: ResultScope
    published_at: datetime | None = None


# ---------- Catalog ----------


class CatalogColumnResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    column_name: str
    inferred_type: str
    description: str
    is_pii: bool
    pii_reason: str | None = None
    stats: dict = {}


class CatalogColumnUpdate(BaseModel):
    column_name: str
    is_pii: bool | None = None
    description: str | None = None


class CatalogDeriveResponse(BaseModel):
    dataset_id: str
    columns: list[CatalogColumnResponse]


# ---------- Proposal ----------


class ProposalCreate(BaseModel):
    dataset_id: str
    title: str
    summary: str
    execution_command: str | None = None
    expected_outputs: list[str] | None = None


class ProposalResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    proposal_id: str
    dataset_id: int
    user_id: int
    title: str
    summary: str
    code_path: str
    report_path: str
    execution_command: str | None = None
    expected_outputs: str | None = None
    status: ProposalStatus
    created_at: datetime
    updated_at: datetime


class ProposalListResponse(BaseModel):
    proposals: list[ProposalResponse]


class ReviewCommentCreate(BaseModel):
    action: ReviewAction
    comment: str


class ReviewCommentResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    proposal_id: int
    reviewer_user_id: int
    action: ReviewAction
    comment: str
    created_at: datetime


# ---------- CSV Validation ----------


class CsvValidationError(BaseModel):
    file_name: str
    errors: list[str]
