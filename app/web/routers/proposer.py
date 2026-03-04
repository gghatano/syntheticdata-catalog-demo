import csv
from pathlib import Path

from fastapi import APIRouter, Depends, Request
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models import Proposal
from app.db.session import get_db
from app.dependencies import get_current_user, get_file_store
from app.services.data_request_service import DataRequestService
from app.services.dataset_service import DatasetService
from app.services.proposal_service import ProposalService
from app.services.submission_service import SubmissionService
from app.services.profiling_service import ProfilingService
from app.services.synthetic_service import SyntheticService
from app.services.template_service import TemplateService
from app.storage.file_store import FileStore

router = APIRouter()
templates = Jinja2Templates(directory="app/web/templates")


@router.get("/datasets")
def dataset_list(
    request: Request,
    q: str | None = None,
    tag: str | None = None,
    sort: str = "created_at",
    user=Depends(get_current_user),
    db: Session = Depends(get_db),
    file_store: FileStore = Depends(get_file_store),
):
    svc = DatasetService(db, file_store)
    tags_filter = [tag] if tag else None
    datasets = svc.search_datasets(query=q, tags=tags_filter, sort_by=sort)

    # Collect all unique tags for filter chips
    all_tags: set[str] = set()
    for ds in datasets:
        for t in ds.tags:
            all_tags.add(t.tag_name)

    ctx = {
        "request": request,
        "datasets": datasets,
        "user": user,
        "q": q or "",
        "current_tag": tag or "",
        "sort": sort,
        "all_tags": sorted(all_tags),
    }

    if request.headers.get("HX-Request"):
        return templates.TemplateResponse("fragments/dataset_list.html", ctx)
    return templates.TemplateResponse("proposer/datasets.html", ctx)


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
    quality_summary = None
    try:
        syn_svc = SyntheticService(db, file_store)
        quality_report = syn_svc.get_quality_report(dataset_id, user.user_id)
        if quality_report:
            from app.synthetic.quality_report import QualityReporter
            reporter = QualityReporter()
            quality_summary = reporter.generate_plain_summary(quality_report)
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
            "quality_summary": quality_summary,
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




@router.get("/datasets/{dataset_id}/template/download")
def download_template(
    dataset_id: str,
    user=Depends(get_current_user),
    db: Session = Depends(get_db),
    file_store: FileStore = Depends(get_file_store),
):
    svc = TemplateService(db, file_store)
    zip_bytes = svc.generate_template_zip(dataset_id, user.user_id)
    return Response(
        content=zip_bytes,
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename=template-{dataset_id}.zip"},
    )


@router.get("/datasets/{dataset_id}/profile")
def dataset_profile(
    dataset_id: str,
    user=Depends(get_current_user),
    db: Session = Depends(get_db),
    file_store: FileStore = Depends(get_file_store),
):
    svc = ProfilingService(db, file_store)
    profiles = svc.get_profile_data(dataset_id, user.user_id)
    return JSONResponse(content=profiles)

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
    purpose = form.get("purpose", "").strip() or None
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
            purpose=purpose,
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


# ---------- Data Requests ----------


@router.get("/data-requests")
def data_request_list(
    request: Request,
    user=Depends(get_current_user),
    db: Session = Depends(get_db),
    file_store: FileStore = Depends(get_file_store),
):
    svc = DataRequestService(db)
    status_filter = request.query_params.get("status")
    requests_list = svc.list_requests(status_filter=status_filter)

    showcase_proposals = ProposalService(db, file_store).list_showcase_proposals()

    return templates.TemplateResponse(
        "data_requests.html",
        {
            "request": request,
            "user": user,
            "data_requests": requests_list,
            "showcase_proposals": showcase_proposals,
        },
    )


@router.post("/data-requests")
async def data_request_create(
    request: Request,
    user=Depends(get_current_user),
    db: Session = Depends(get_db),
):
    form = await request.form()
    title = form.get("title", "").strip()
    description = form.get("description", "").strip()
    desired_columns = form.get("desired_columns", "").strip() or None
    showcase_proposal_id_str = form.get("showcase_proposal_id", "").strip()
    showcase_proposal_id = int(showcase_proposal_id_str) if showcase_proposal_id_str else None

    if not title or not description:
        return RedirectResponse(url="/proposer/data-requests", status_code=303)

    svc = DataRequestService(db)
    svc.create_request(user.user_id, title, description, desired_columns, showcase_proposal_id)
    return RedirectResponse(url="/proposer/data-requests", status_code=303)


@router.post("/data-requests/{request_id}/vote")
def data_request_vote(
    request_id: str,
    request: Request,
    user=Depends(get_current_user),
    db: Session = Depends(get_db),
):
    svc = DataRequestService(db)
    svc.vote(request_id, user.user_id)
    return RedirectResponse(url="/proposer/data-requests", status_code=303)
