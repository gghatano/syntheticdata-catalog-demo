from fastapi import Depends, HTTPException, Request
from sqlalchemy.orm import Session

from app.db.models import User, UserRole
from app.db.session import get_db
from app.services.auth_service import AuthService


def get_current_api_user(request: Request, db: Session = Depends(get_db)) -> User:
    """Retrieve the current user from the session for API endpoints.

    Returns 401 instead of redirecting (unlike the web dependency).
    """
    user_id = request.session.get("user_id")
    if not user_id:
        raise HTTPException(status_code=401, detail="Not authenticated")
    auth = AuthService(db)
    user = auth.get_user(user_id)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return user


def require_api_hr(user: User = Depends(get_current_api_user)) -> User:
    """Require that the current user has HR role."""
    if user.role != UserRole.hr:
        raise HTTPException(status_code=403, detail="HR role required")
    return user
