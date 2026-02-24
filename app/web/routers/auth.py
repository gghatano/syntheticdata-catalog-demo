from fastapi import APIRouter, Depends, Request
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.services.auth_service import AuthService

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

    if user.role.value == "hr":
        return RedirectResponse(url="/hr/datasets", status_code=303)
    return RedirectResponse(url="/proposer/datasets", status_code=303)


@router.get("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/login", status_code=303)
