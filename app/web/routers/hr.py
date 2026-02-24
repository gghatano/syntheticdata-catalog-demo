from fastapi import APIRouter, Depends, Request, UploadFile, File, Form
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.dependencies import get_file_store, require_hr
from app.services.dataset_service import DatasetService
from app.services.submission_service import SubmissionService
from app.services.execution_service import ExecutionService
from app.services.synthetic_service import SyntheticService
from app.storage.file_store import FileStore

router = APIRouter()
templates = Jinja2Templates(directory="app/web/templates")


@router.get("/datasets")
def dataset_list(
    request: Request,
    user=Depends(require_hr),
    db: Session = Depends(get_db),
    file_store: FileStore = Depends(get_file_store),
):
    svc = DatasetService(db, file_store)
    datasets = svc.list_datasets_for_owner(user.user_id)

    if request.headers.get("HX-Request"):
        return templates.TemplateResponse(
            "fragments/dataset_list.html",
            {"request": request, "datasets": datasets, "user": user},
        )
    return templates.TemplateResponse(
        "hr/datasets.html",
        {"request": request, "datasets": datasets, "user": user},
    )


@router.get("/datasets/{dataset_id}")
def dataset_detail(
    dataset_id: str,
    request: Request,
    user=Depends(require_hr),
    db: Session = Depends(get_db),
    file_store: FileStore = Depends(get_file_store),
):
    svc = DatasetService(db, file_store)
    dataset = svc.get_dataset(dataset_id, user.user_id)

    quality_report = None
    try:
        syn_svc = SyntheticService(db, file_store)
        quality_report = syn_svc.get_quality_report(dataset_id, user.user_id)
    except (ValueError, PermissionError):
        pass

    return templates.TemplateResponse(
        "hr/dataset_detail.html",
        {
            "request": request,
            "dataset": dataset,
            "user": user,
            "quality_report": quality_report,
        },
    )


@router.post("/datasets")
async def dataset_create(
    request: Request,
    user=Depends(require_hr),
    db: Session = Depends(get_db),
    file_store: FileStore = Depends(get_file_store),
):
    form = await request.form()
    name = form.get("name", "").strip()
    if not name:
        return RedirectResponse(url="/hr/datasets", status_code=303)

    files: dict[str, bytes] = {}
    for file_type in ("employee_master", "project_allocation", "working_hours"):
        upload = form.get(file_type)
        if upload and hasattr(upload, "read"):
            content = await upload.read()
            if content:
                files[file_type] = content

    if not files:
        return RedirectResponse(url="/hr/datasets", status_code=303)

    svc = DatasetService(db, file_store)
    svc.create_dataset(user.user_id, name, files)
    return RedirectResponse(url="/hr/datasets", status_code=303)


@router.post("/datasets/{dataset_id}/synthetic")
def generate_synthetic(
    dataset_id: str,
    request: Request,
    user=Depends(require_hr),
    db: Session = Depends(get_db),
    file_store: FileStore = Depends(get_file_store),
):
    svc = SyntheticService(db, file_store)
    svc.generate(dataset_id, user.user_id)
    return RedirectResponse(url=f"/hr/datasets/{dataset_id}", status_code=303)


@router.post("/datasets/{dataset_id}/publish")
def publish_dataset(
    dataset_id: str,
    request: Request,
    user=Depends(require_hr),
    db: Session = Depends(get_db),
    file_store: FileStore = Depends(get_file_store),
):
    svc = SyntheticService(db, file_store)
    svc.publish(dataset_id, user.user_id, True)
    return RedirectResponse(url=f"/hr/datasets/{dataset_id}", status_code=303)


@router.get("/submissions")
def submission_list(
    request: Request,
    user=Depends(require_hr),
    db: Session = Depends(get_db),
    file_store: FileStore = Depends(get_file_store),
):
    # List all submissions across all datasets owned by this HR user
    ds_svc = DatasetService(db, file_store)
    datasets = ds_svc.list_datasets_for_owner(user.user_id)

    sub_svc = SubmissionService(db, file_store)
    all_submissions = []
    for ds in datasets:
        subs = sub_svc.list_submissions(ds.dataset_id, user.user_id)
        all_submissions.extend(subs)

    if request.headers.get("HX-Request"):
        return templates.TemplateResponse(
            "fragments/submission_list.html",
            {"request": request, "submissions": all_submissions, "user": user},
        )
    return templates.TemplateResponse(
        "hr/submissions.html",
        {"request": request, "submissions": all_submissions, "user": user},
    )


@router.post("/submissions/{submission_id}/approve")
def approve_submission(
    submission_id: str,
    request: Request,
    user=Depends(require_hr),
    db: Session = Depends(get_db),
    file_store: FileStore = Depends(get_file_store),
):
    svc = SubmissionService(db, file_store)
    svc.approve_submission(submission_id, user.user_id)
    return RedirectResponse(url="/hr/submissions", status_code=303)


@router.post("/submissions/{submission_id}/reject")
async def reject_submission(
    submission_id: str,
    request: Request,
    user=Depends(require_hr),
    db: Session = Depends(get_db),
    file_store: FileStore = Depends(get_file_store),
):
    form = await request.form()
    reason = form.get("reason", "").strip() or "理由なし"
    svc = SubmissionService(db, file_store)
    svc.reject_submission(submission_id, user.user_id, reason)
    return RedirectResponse(url="/hr/submissions", status_code=303)


@router.post("/executions/{submission_id}/run")
async def run_execution(
    submission_id: str,
    request: Request,
    user=Depends(require_hr),
    db: Session = Depends(get_db),
    file_store: FileStore = Depends(get_file_store),
):
    form = await request.form()
    mode = form.get("mode", "synthetic")
    svc = ExecutionService(db, file_store)
    execution = svc.run_submission(submission_id, user.user_id, mode)
    return RedirectResponse(
        url=f"/hr/executions/{execution.execution_id}", status_code=303
    )


@router.get("/executions/{execution_id}")
def execution_detail(
    execution_id: str,
    request: Request,
    user=Depends(require_hr),
    db: Session = Depends(get_db),
    file_store: FileStore = Depends(get_file_store),
):
    svc = ExecutionService(db, file_store)
    execution = svc.get_execution(execution_id, user.user_id)

    if request.headers.get("HX-Request"):
        return templates.TemplateResponse(
            "fragments/execution_result.html",
            {"request": request, "execution": execution, "user": user},
        )
    return templates.TemplateResponse(
        "hr/execution_detail.html",
        {"request": request, "execution": execution, "user": user},
    )
