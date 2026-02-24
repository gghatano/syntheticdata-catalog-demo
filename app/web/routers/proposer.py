from fastapi import APIRouter, Depends, Request
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.dependencies import get_current_user, get_file_store
from app.services.dataset_service import DatasetService
from app.services.submission_service import SubmissionService
from app.services.synthetic_service import SyntheticService
from app.storage.file_store import FileStore

router = APIRouter()
templates = Jinja2Templates(directory="app/web/templates")


@router.get("/datasets")
def dataset_list(
    request: Request,
    user=Depends(get_current_user),
    db: Session = Depends(get_db),
    file_store: FileStore = Depends(get_file_store),
):
    svc = DatasetService(db, file_store)
    datasets = svc.list_published_datasets()

    if request.headers.get("HX-Request"):
        return templates.TemplateResponse(
            "fragments/dataset_list.html",
            {"request": request, "datasets": datasets, "user": user},
        )
    return templates.TemplateResponse(
        "proposer/datasets.html",
        {"request": request, "datasets": datasets, "user": user},
    )


@router.get("/datasets/{dataset_id}")
def dataset_detail(
    dataset_id: str,
    request: Request,
    user=Depends(get_current_user),
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
        "proposer/dataset_detail.html",
        {
            "request": request,
            "dataset": dataset,
            "user": user,
            "quality_report": quality_report,
        },
    )


@router.get("/submissions/new")
def submission_form(
    request: Request,
    user=Depends(get_current_user),
    db: Session = Depends(get_db),
    file_store: FileStore = Depends(get_file_store),
):
    svc = DatasetService(db, file_store)
    datasets = svc.list_published_datasets()
    return templates.TemplateResponse(
        "proposer/submission_form.html",
        {"request": request, "datasets": datasets, "user": user},
    )


@router.post("/submissions")
async def submission_create(
    request: Request,
    user=Depends(get_current_user),
    db: Session = Depends(get_db),
    file_store: FileStore = Depends(get_file_store),
):
    form = await request.form()
    dataset_id = form.get("dataset_id", "").strip()
    title = form.get("title", "").strip()
    description = form.get("description", "").strip()
    zip_file = form.get("zip_file")

    if not all([dataset_id, title, description]) or not zip_file or not hasattr(zip_file, "read"):
        return RedirectResponse(url="/proposer/submissions/new", status_code=303)

    zip_content = await zip_file.read()
    svc = SubmissionService(db, file_store)
    svc.create_submission(user.user_id, dataset_id, title, description, zip_content)
    return RedirectResponse(url="/proposer/submissions", status_code=303)


@router.get("/submissions")
def submission_list(
    request: Request,
    user=Depends(get_current_user),
    db: Session = Depends(get_db),
    file_store: FileStore = Depends(get_file_store),
):
    ds_svc = DatasetService(db, file_store)
    datasets = ds_svc.list_published_datasets()

    sub_svc = SubmissionService(db, file_store)
    all_submissions = []
    for ds in datasets:
        try:
            subs = sub_svc.list_submissions(ds.dataset_id, user.user_id)
            all_submissions.extend(subs)
        except (ValueError, PermissionError):
            pass

    if request.headers.get("HX-Request"):
        return templates.TemplateResponse(
            "fragments/submission_list.html",
            {"request": request, "submissions": all_submissions, "user": user},
        )
    return templates.TemplateResponse(
        "proposer/submissions.html",
        {"request": request, "submissions": all_submissions, "user": user},
    )


@router.get("/submissions/{submission_id}")
def submission_detail(
    submission_id: str,
    request: Request,
    user=Depends(get_current_user),
    db: Session = Depends(get_db),
    file_store: FileStore = Depends(get_file_store),
):
    svc = SubmissionService(db, file_store)
    submission = svc.get_submission(submission_id, user.user_id)
    return templates.TemplateResponse(
        "proposer/submission_detail.html",
        {"request": request, "submission": submission, "user": user},
    )
