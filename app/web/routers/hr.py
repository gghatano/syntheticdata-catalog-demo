from pathlib import Path

from fastapi import APIRouter, Depends, Request
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models import Proposal
from app.db.session import get_db
from app.dependencies import get_file_store, require_hr
from app.services.catalog_service import CatalogService
from app.services.dataset_service import DatasetService
from app.services.execution_service import ExecutionService
from app.services.proposal_service import ProposalService
from app.services.submission_service import SubmissionService
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

    # Get catalog columns
    catalog_columns = []
    try:
        cat_svc = CatalogService(db, file_store)
        catalog_columns = cat_svc.get_catalog(dataset_id, user.user_id)
    except (ValueError, PermissionError):
        pass

    return templates.TemplateResponse(
        "hr/dataset_detail.html",
        {
            "request": request,
            "dataset": dataset,
            "user": user,
            "quality_report": quality_report,
            "catalog_columns": catalog_columns,
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


# ---------- Catalog ----------


@router.get("/datasets/{dataset_id}/catalog")
def catalog_edit(
    dataset_id: str,
    request: Request,
    user=Depends(require_hr),
    db: Session = Depends(get_db),
    file_store: FileStore = Depends(get_file_store),
):
    ds_svc = DatasetService(db, file_store)
    dataset = ds_svc.get_dataset(dataset_id, user.user_id)

    cat_svc = CatalogService(db, file_store)
    columns = cat_svc.get_catalog(dataset_id, user.user_id)

    message = request.query_params.get("message")
    error = request.query_params.get("error")

    return templates.TemplateResponse(
        "hr/catalog_edit.html",
        {
            "request": request,
            "user": user,
            "dataset": dataset,
            "columns": columns,
            "message": message,
            "error": error,
        },
    )


@router.post("/datasets/{dataset_id}/catalog/derive")
def catalog_derive(
    dataset_id: str,
    request: Request,
    user=Depends(require_hr),
    db: Session = Depends(get_db),
    file_store: FileStore = Depends(get_file_store),
):
    cat_svc = CatalogService(db, file_store)
    try:
        cat_svc.derive_catalog(dataset_id, user.user_id)
        return RedirectResponse(
            url=f"/hr/datasets/{dataset_id}/catalog?message=カタログを自動生成しました",
            status_code=303,
        )
    except (ValueError, PermissionError) as e:
        return RedirectResponse(
            url=f"/hr/datasets/{dataset_id}/catalog?error={e}",
            status_code=303,
        )


@router.post("/datasets/{dataset_id}/catalog")
async def catalog_update(
    dataset_id: str,
    request: Request,
    user=Depends(require_hr),
    db: Session = Depends(get_db),
    file_store: FileStore = Depends(get_file_store),
):
    form = await request.form()
    column_names = form.getlist("column_name")

    columns_update = []
    for col_name in column_names:
        is_pii = form.get(f"is_pii_{col_name}") == "true"
        description = form.get(f"description_{col_name}", "")
        columns_update.append({
            "column_name": col_name,
            "is_pii": is_pii,
            "description": description,
        })

    cat_svc = CatalogService(db, file_store)
    try:
        cat_svc.update_catalog(dataset_id, columns_update, user.user_id)
        return RedirectResponse(
            url=f"/hr/datasets/{dataset_id}/catalog?message=カタログを保存しました",
            status_code=303,
        )
    except (ValueError, PermissionError) as e:
        return RedirectResponse(
            url=f"/hr/datasets/{dataset_id}/catalog?error={e}",
            status_code=303,
        )


# ---------- Submissions ----------


@router.get("/submissions")
def submission_list(
    request: Request,
    user=Depends(require_hr),
    db: Session = Depends(get_db),
    file_store: FileStore = Depends(get_file_store),
):
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


# ---------- Proposals (HR Review) ----------


@router.get("/proposals")
def proposal_list(
    request: Request,
    user=Depends(require_hr),
    db: Session = Depends(get_db),
):
    proposals = list(
        db.execute(
            select(Proposal).order_by(Proposal.created_at.desc())
        ).scalars().all()
    )
    return templates.TemplateResponse(
        "hr/proposal_list.html",
        {"request": request, "user": user, "proposals": proposals},
    )


@router.get("/proposals/{proposal_id}")
def proposal_detail(
    proposal_id: str,
    request: Request,
    user=Depends(require_hr),
    db: Session = Depends(get_db),
    file_store: FileStore = Depends(get_file_store),
):
    svc = ProposalService(db, file_store)
    proposal = svc.get_proposal(proposal_id, user.user_id)
    comments = svc.get_review_comments(proposal_id, user.user_id)

    # Read proposal files
    code_content = None
    report_content = None
    try:
        code_path = Path(proposal.code_path)
        if code_path.exists():
            code_content = code_path.read_text(encoding="utf-8")
    except Exception:
        pass
    try:
        report_path = Path(proposal.report_path)
        if report_path.exists():
            report_content = report_path.read_text(encoding="utf-8")
    except Exception:
        pass

    return templates.TemplateResponse(
        "hr/proposal_detail.html",
        {
            "request": request,
            "user": user,
            "proposal": proposal,
            "comments": comments,
            "code_content": code_content,
            "report_content": report_content,
        },
    )


@router.post("/proposals/{proposal_id}/review")
async def proposal_review(
    proposal_id: str,
    request: Request,
    user=Depends(require_hr),
    db: Session = Depends(get_db),
    file_store: FileStore = Depends(get_file_store),
):
    form = await request.form()
    action = form.get("action", "comment")
    comment = form.get("comment", "").strip()
    if not comment:
        comment = "コメントなし"

    svc = ProposalService(db, file_store)
    svc.review_proposal(proposal_id, user.user_id, action, comment)
    return RedirectResponse(
        url=f"/hr/proposals/{proposal_id}", status_code=303
    )
