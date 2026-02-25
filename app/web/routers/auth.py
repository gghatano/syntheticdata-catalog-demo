from fastapi import APIRouter, Depends, Request
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models import Proposal, ProposalStatus, UserRole
from app.db.session import get_db
from app.dependencies import get_current_user, get_file_store
from app.services.auth_service import AuthService
from app.services.dataset_service import DatasetService
from app.storage.file_store import FileStore

router = APIRouter()
templates = Jinja2Templates(directory="app/web/templates")


@router.get("/login")
def login_form(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})


@router.post("/login")
async def login_post(request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    user_id = form.get("user_id", "").strip()

    if not user_id:
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "error": "ユーザーIDを入力してください"},
        )

    auth = AuthService(db)
    user = auth.get_user(user_id)
    if not user:
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "error": "ユーザーが見つかりません"},
        )

    request.session["user_id"] = user.user_id
    return RedirectResponse(url="/dashboard", status_code=303)


@router.get("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/login", status_code=303)


@router.get("/dashboard")
def dashboard(
    request: Request,
    user=Depends(get_current_user),
    db: Session = Depends(get_db),
    file_store: FileStore = Depends(get_file_store),
):
    ds_svc = DatasetService(db, file_store)

    if user.role == UserRole.hr:
        datasets = ds_svc.list_datasets_for_owner(user.user_id)
        # Get pending proposals across all datasets
        pending_proposals = list(
            db.execute(
                select(Proposal).where(
                    Proposal.status == ProposalStatus.submitted
                ).order_by(Proposal.created_at.desc())
            ).scalars().all()
        )
        return templates.TemplateResponse(
            "dashboard.html",
            {
                "request": request,
                "user": user,
                "datasets": datasets,
                "pending_proposals": pending_proposals,
            },
        )
    else:
        datasets = ds_svc.list_published_datasets()
        # Get this user's proposals
        my_proposals = list(
            db.execute(
                select(Proposal).where(
                    Proposal.user_id == user.id
                ).order_by(Proposal.created_at.desc())
            ).scalars().all()
        )
        return templates.TemplateResponse(
            "dashboard.html",
            {
                "request": request,
                "user": user,
                "datasets": datasets,
                "my_proposals": my_proposals,
            },
        )
