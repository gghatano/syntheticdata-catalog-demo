from datetime import datetime

from pydantic import BaseModel, ConfigDict

from app.db.models import (
    ExecutionMode,
    ExecutionStatus,
    FileType,
    ResultScope,
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
    owner_user_id: str
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


# ---------- CSV Validation ----------


class CsvValidationError(BaseModel):
    file_name: str
    errors: list[str]
