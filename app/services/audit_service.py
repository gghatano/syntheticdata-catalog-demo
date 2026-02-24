from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models import AuditLog


class AuditLogService:
    def __init__(self, db: Session):
        self.db = db

    def log(
        self,
        user_id: str,
        action: str,
        target_type: str,
        target_id: str,
        detail: str | None = None,
    ) -> AuditLog:
        """監査ログ記録"""
        entry = AuditLog(
            user_id=user_id,
            action=action,
            target_type=target_type,
            target_id=target_id,
            detail=detail,
        )
        self.db.add(entry)
        self.db.commit()
        self.db.refresh(entry)
        return entry

    def list_logs(
        self,
        target_type: str | None = None,
        target_id: str | None = None,
    ) -> list[AuditLog]:
        """監査ログ一覧"""
        stmt = select(AuditLog)
        if target_type is not None:
            stmt = stmt.where(AuditLog.target_type == target_type)
        if target_id is not None:
            stmt = stmt.where(AuditLog.target_id == target_id)
        stmt = stmt.order_by(AuditLog.created_at.desc())
        return list(self.db.execute(stmt).scalars().all())
