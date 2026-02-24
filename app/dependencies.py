from fastapi import Depends, Request, HTTPException
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.storage.file_store import FileStore
from app.services.auth_service import AuthService


class RequiresLoginException(Exception):
    """未認証ユーザーをログインページにリダイレクトするための例外"""
    pass


def get_file_store() -> FileStore:
    return FileStore()


def get_current_user(request: Request, db: Session = Depends(get_db)):
    """セッションCookieからuser_idを取得し、ユーザー検索。未認証なら/loginにリダイレクト"""
    user_id = request.session.get("user_id")
    if not user_id:
        raise RequiresLoginException()
    auth = AuthService(db)
    user = auth.get_user(user_id)
    if not user:
        raise RequiresLoginException()
    return user


def require_hr(user=Depends(get_current_user)):
    if user.role.value != "hr":
        raise HTTPException(status_code=403, detail="HR権限が必要です")
    return user
