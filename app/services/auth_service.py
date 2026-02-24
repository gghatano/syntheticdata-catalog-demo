from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models import User, UserRole


class AuthService:
    def __init__(self, db: Session):
        self.db = db

    def create_user(self, user_id: str, display_name: str, role: str) -> User:
        """ユーザー作成。user_idが重複する場合はエラー。"""
        existing = self.db.execute(
            select(User).where(User.user_id == user_id)
        ).scalar_one_or_none()
        if existing is not None:
            raise ValueError(f"User already exists: {user_id}")

        user = User(
            user_id=user_id,
            display_name=display_name,
            role=UserRole(role),
        )
        self.db.add(user)
        self.db.commit()
        self.db.refresh(user)
        return user

    def get_user(self, user_id: str) -> User | None:
        """user_idでユーザー取得"""
        return self.db.execute(
            select(User).where(User.user_id == user_id)
        ).scalar_one_or_none()

    def authenticate(self, user_id: str) -> User:
        """user_idでユーザー検索。存在しなければ例外。MVP向け簡易認証。"""
        user = self.get_user(user_id)
        if user is None:
            raise ValueError(f"User not found: {user_id}")
        return user

    def check_role(self, user: User, required_role: str) -> None:
        """ロールチェック。不一致なら PermissionError"""
        if user.role.value != required_role:
            raise PermissionError(
                f"Required role '{required_role}', but user '{user.user_id}' has role '{user.role.value}'"
            )

    def seed_users(self) -> list[User]:
        """デモ用初期ユーザー作成"""
        seed_data = [
            ("hr_demo", "HR デモユーザー", "hr"),
            ("user_demo_01", "提案者デモ01", "proposer"),
            ("user_demo_02", "提案者デモ02", "proposer"),
            ("admin_demo", "管理者デモ", "admin"),
        ]
        users: list[User] = []
        for user_id, display_name, role in seed_data:
            existing = self.get_user(user_id)
            if existing is not None:
                users.append(existing)
                continue
            users.append(self.create_user(user_id, display_name, role))
        return users
