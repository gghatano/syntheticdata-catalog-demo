import csv
from pathlib import Path

from fastapi import APIRouter, Depends, Request
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models import Proposal
from app.db.session import get_db
from app.dependencies import get_current_user, get_file_store
from app.services.dataset_service import DatasetService
from app.services.proposal_service import ProposalService
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

    # Get synthetic data preview (first 10 rows)
    preview_tables = []
    for artifact in dataset.artifacts:
        artifact_path = Path(artifact.file_path)
        if artifact_path.exists() and artifact_path.suffix == ".csv":
            try:
                with open(artifact_path, encoding="utf-8") as f:
                    reader = csv.reader(f)
                    headers = next(reader, [])
                    rows = []
                    for i, row in enumerate(reader):
                        if i >= 10:
                            break
                        rows.append(row)
                    if headers:
                        preview_tables.append({
                            "file_type": artifact.file_type.value,
                            "headers": headers,
                            "rows": rows,
                        })
            except Exception:
                pass

    return templates.TemplateResponse(
        "proposer/dataset_detail.html",
        {
            "request": request,
            "dataset": dataset,
            "user": user,
            "quality_report": quality_report,
            "preview_tables": preview_tables,
        },
    )


@router.get("/datasets/{dataset_id}/synthetic/download")
def download_synthetic(
    dataset_id: str,
    user=Depends(get_current_user),
    db: Session = Depends(get_db),
    file_store: FileStore = Depends(get_file_store),
):
    svc = DatasetService(db, file_store)
    dataset = svc.get_dataset(dataset_id, user.user_id)

    # Find the first artifact to download
    if not dataset.artifacts:
        return RedirectResponse(
            url=f"/proposer/datasets/{dataset_id}", status_code=303
        )

    artifact = dataset.artifacts[0]
    artifact_path = Path(artifact.file_path)
    if not artifact_path.exists():
        return RedirectResponse(
            url=f"/proposer/datasets/{dataset_id}", status_code=303
        )

    return FileResponse(
        path=str(artifact_path),
        filename=f"{dataset.name}_{artifact.file_type.value}.csv",
        media_type="text/csv",
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


# ---------- Proposals ----------


@router.get("/proposals")
def proposal_list(
    request: Request,
    user=Depends(get_current_user),
    db: Session = Depends(get_db),
):
    proposals = list(
        db.execute(
            select(Proposal).where(
                Proposal.user_id == user.id
            ).order_by(Proposal.created_at.desc())
        ).scalars().all()
    )
    return templates.TemplateResponse(
        "proposer/proposal_list.html",
        {"request": request, "user": user, "proposals": proposals},
    )


@router.get("/proposals/new")
def proposal_form(
    request: Request,
    user=Depends(get_current_user),
    db: Session = Depends(get_db),
    file_store: FileStore = Depends(get_file_store),
):
    svc = DatasetService(db, file_store)
    datasets = svc.list_published_datasets()
    return templates.TemplateResponse(
        "proposer/proposal_form.html",
        {"request": request, "user": user, "datasets": datasets},
    )


@router.post("/proposals")
async def proposal_create(
    request: Request,
    user=Depends(get_current_user),
    db: Session = Depends(get_db),
    file_store: FileStore = Depends(get_file_store),
):
    form = await request.form()
    dataset_id = form.get("dataset_id", "").strip()
    title = form.get("title", "").strip()
    summary = form.get("summary", "").strip()
    code_file = form.get("code_file")
    report_file = form.get("report_file")

    if not all([dataset_id, title, summary]):
        return templates.TemplateResponse(
            "proposer/proposal_form.html",
            {
                "request": request,
                "user": user,
                "datasets": DatasetService(db, file_store).list_published_datasets(),
                "error": "全ての項目を入力してください",
            },
        )

    if not code_file or not hasattr(code_file, "read"):
        return templates.TemplateResponse(
            "proposer/proposal_form.html",
            {
                "request": request,
                "user": user,
                "datasets": DatasetService(db, file_store).list_published_datasets(),
                "error": "analysis.pyファイルをアップロードしてください",
            },
        )

    if not report_file or not hasattr(report_file, "read"):
        return templates.TemplateResponse(
            "proposer/proposal_form.html",
            {
                "request": request,
                "user": user,
                "datasets": DatasetService(db, file_store).list_published_datasets(),
                "error": "report.mdファイルをアップロードしてください",
            },
        )

    code_content = await code_file.read()
    report_content = await report_file.read()

    svc = ProposalService(db, file_store)
    try:
        svc.create_proposal(
            actor_user_id=user.user_id,
            dataset_id=dataset_id,
            title=title,
            summary=summary,
            code_content=code_content,
            report_content=report_content,
        )
    except (ValueError, PermissionError) as e:
        return templates.TemplateResponse(
            "proposer/proposal_form.html",
            {
                "request": request,
                "user": user,
                "datasets": DatasetService(db, file_store).list_published_datasets(),
                "error": str(e),
            },
        )

    return RedirectResponse(url="/proposer/proposals", status_code=303)


@router.get("/proposals/{proposal_id}")
def proposal_detail(
    proposal_id: str,
    request: Request,
    user=Depends(get_current_user),
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
        "proposer/proposal_detail.html",
        {
            "request": request,
            "user": user,
            "proposal": proposal,
            "comments": comments,
            "code_content": code_content,
            "report_content": report_content,
        },
    )
