from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from app.api.auth import get_current_api_user, require_api_hr
from app.db.models import User
from app.db.session import get_db
from app.dependencies import get_file_store
from app.schemas.dto import (
    CatalogColumnResponse,
    CatalogColumnUpdate,
    CatalogDeriveResponse,
    DatasetCreate,
    DatasetListResponse,
    DatasetResponse,
    QualityReportResponse,
)
from app.services.catalog_service import CatalogService
from app.services.dataset_service import DatasetService
from app.services.synthetic_service import SyntheticService
from app.storage.file_store import FileStore

router = APIRouter()


# ------------------------------------------------------------------
# Dataset CRUD
# ------------------------------------------------------------------


@router.post("", response_model=DatasetResponse, status_code=201)
def create_dataset(
    body: DatasetCreate,
    db: Session = Depends(get_db),
    user: User = Depends(require_api_hr),
    file_store: FileStore = Depends(get_file_store),
):
    """Create a new dataset (metadata only, no files yet)."""
    try:
        svc = DatasetService(db, file_store)
        ds = svc.create_dataset(user.user_id, body.name, {})
        return ds
    except (ValueError, PermissionError) as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("", response_model=DatasetListResponse)
def list_datasets(
    db: Session = Depends(get_db),
    user: User = Depends(get_current_api_user),
    file_store: FileStore = Depends(get_file_store),
):
    """List datasets. HR sees all owned; others see published only."""
    svc = DatasetService(db, file_store)
    from app.db.models import UserRole
    if user.role == UserRole.hr:
        datasets = svc.list_datasets_for_owner(user.user_id)
    else:
        datasets = svc.list_published_datasets()
    return DatasetListResponse(datasets=datasets)


@router.get("/{dataset_id}", response_model=DatasetResponse)
def get_dataset(
    dataset_id: str,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_api_user),
    file_store: FileStore = Depends(get_file_store),
):
    """Get a single dataset by ID."""
    try:
        svc = DatasetService(db, file_store)
        return svc.get_dataset(dataset_id, user.user_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))


# ------------------------------------------------------------------
# Upload CSV
# ------------------------------------------------------------------


@router.post("/{dataset_id}/upload", response_model=DatasetResponse)
def upload_csv(
    dataset_id: str,
    file: UploadFile = File(...),
    file_type: str = "employee_master",
    db: Session = Depends(get_db),
    user: User = Depends(require_api_hr),
    file_store: FileStore = Depends(get_file_store),
):
    """Upload a CSV file to an existing dataset."""
    try:
        content = file.file.read()
        svc = DatasetService(db, file_store)
        ds = svc.get_dataset(dataset_id, user.user_id)
        # Save file via file_store and add DatasetFile record
        from app.db.models import DatasetFile, FileType
        filename = f"{file_type}.csv"
        saved_path = file_store.save_real_data(dataset_id, file_type, content, filename)
        df = DatasetFile(
            dataset_id=ds.id,
            file_type=FileType(file_type),
            file_path=str(saved_path),
        )
        db.add(df)
        db.commit()
        db.refresh(ds)
        return ds
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))


# ------------------------------------------------------------------
# Catalog
# ------------------------------------------------------------------


@router.post("/{dataset_id}/catalog/derive", response_model=CatalogDeriveResponse)
def derive_catalog(
    dataset_id: str,
    db: Session = Depends(get_db),
    user: User = Depends(require_api_hr),
    file_store: FileStore = Depends(get_file_store),
):
    """Derive catalog columns from uploaded CSV data."""
    try:
        svc = CatalogService(db, file_store)
        columns = svc.derive_catalog(dataset_id, user.user_id)
        import json
        col_responses = [
            CatalogColumnResponse(
                column_name=c.column_name,
                inferred_type=c.inferred_type,
                description=c.description,
                is_pii=c.is_pii,
                pii_reason=c.pii_reason,
                stats=json.loads(c.stats_json) if c.stats_json else {},
            )
            for c in columns
        ]
        return CatalogDeriveResponse(dataset_id=dataset_id, columns=col_responses)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))


@router.put("/{dataset_id}/catalog", response_model=CatalogDeriveResponse)
def update_catalog(
    dataset_id: str,
    columns: list[CatalogColumnUpdate],
    db: Session = Depends(get_db),
    user: User = Depends(require_api_hr),
    file_store: FileStore = Depends(get_file_store),
):
    """Update catalog column metadata (PII flags, descriptions)."""
    try:
        svc = CatalogService(db, file_store)
        updated = svc.update_catalog(
            dataset_id,
            [c.model_dump(exclude_none=True) for c in columns],
            user.user_id,
        )
        import json
        col_responses = [
            CatalogColumnResponse(
                column_name=c.column_name,
                inferred_type=c.inferred_type,
                description=c.description,
                is_pii=c.is_pii,
                pii_reason=c.pii_reason,
                stats=json.loads(c.stats_json) if c.stats_json else {},
            )
            for c in updated
        ]
        return CatalogDeriveResponse(dataset_id=dataset_id, columns=col_responses)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))


# ------------------------------------------------------------------
# Synthesize
# ------------------------------------------------------------------


@router.post("/{dataset_id}/synthesize")
def synthesize(
    dataset_id: str,
    seed: int = 42,
    db: Session = Depends(get_db),
    user: User = Depends(require_api_hr),
    file_store: FileStore = Depends(get_file_store),
):
    """Generate synthetic data for a dataset."""
    try:
        svc = SyntheticService(db, file_store)
        artifacts = svc.generate(dataset_id, user.user_id, seed=seed)
        return {
            "dataset_id": dataset_id,
            "artifacts": [
                {
                    "file_type": a.file_type.value,
                    "file_path": a.file_path,
                    "seed": a.seed,
                }
                for a in artifacts
            ],
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))


# ------------------------------------------------------------------
# Quality Report
# ------------------------------------------------------------------


@router.get("/{dataset_id}/reports/latest", response_model=QualityReportResponse)
def get_latest_report(
    dataset_id: str,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_api_user),
    file_store: FileStore = Depends(get_file_store),
):
    """Get the latest quality report for a dataset."""
    try:
        svc = SyntheticService(db, file_store)
        report = svc.get_quality_report(dataset_id, user.user_id)
        return QualityReportResponse(dataset_id=dataset_id, report=report)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))


# ------------------------------------------------------------------
# Publish
# ------------------------------------------------------------------


@router.post("/{dataset_id}/publish", response_model=DatasetResponse)
def publish_dataset(
    dataset_id: str,
    db: Session = Depends(get_db),
    user: User = Depends(require_api_hr),
    file_store: FileStore = Depends(get_file_store),
):
    """Publish a dataset (make synthetic data available to proposers)."""
    try:
        svc = SyntheticService(db, file_store)
        ds = svc.publish(dataset_id, user.user_id, is_public=True)
        return ds
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))


# ------------------------------------------------------------------
# Download synthetic data
# ------------------------------------------------------------------


@router.get("/{dataset_id}/synthetic/download")
def download_synthetic(
    dataset_id: str,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_api_user),
    file_store: FileStore = Depends(get_file_store),
):
    """Download synthetic data CSV files for a dataset."""
    try:
        svc = DatasetService(db, file_store)
        ds = svc.get_dataset(dataset_id, user.user_id)
        if not ds.is_published:
            from app.db.models import UserRole
            if user.role != UserRole.hr:
                raise PermissionError("Dataset is not published")

        synth_dir = file_store.get_synthetic_data_path(dataset_id)
        if not synth_dir.exists():
            raise HTTPException(status_code=404, detail="No synthetic data found")

        csv_files = list(synth_dir.glob("*.csv"))
        if not csv_files:
            raise HTTPException(status_code=404, detail="No synthetic CSV files found")

        # Return first CSV file (single-file case)
        return FileResponse(
            path=str(csv_files[0]),
            media_type="text/csv",
            filename=csv_files[0].name,
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
