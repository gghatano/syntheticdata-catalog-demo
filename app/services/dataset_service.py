from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.db.models import Dataset, DatasetFile, FileType, User, UserRole
from app.storage.file_store import FileStore
from app.utils.ids import generate_id


class DatasetService:
    def __init__(self, db: Session, file_store: FileStore):
        self.db = db
        self.file_store = file_store

    def create_dataset(
        self, owner_user_id: str, name: str, files: dict[str, bytes]
    ) -> Dataset:
        """
        データセット作成。
        1. ownerユーザーの存在確認・hrロールチェック
        2. dataset_id生成 (DS0001形式、DB内のmax+1)
        3. Datasetレコード作成
        4. files辞書 {file_type: file_content} をFileStoreで保存
        5. DatasetFileレコード作成
        6. commit & return
        """
        owner = self._get_user_or_raise(owner_user_id)
        if owner.role != UserRole.hr:
            raise PermissionError(
                f"Only hr users can create datasets. User '{owner_user_id}' has role '{owner.role.value}'"
            )

        dataset_id = self._next_dataset_id()
        dataset = Dataset(
            dataset_id=dataset_id,
            name=name,
            owner_user_id=owner.id,
        )
        self.db.add(dataset)
        self.db.flush()  # get dataset.id

        for file_type_str, file_content in files.items():
            filename = f"{file_type_str}.csv"
            saved_path = self.file_store.save_real_data(
                dataset_id=dataset_id,
                file_type=file_type_str,
                file_content=file_content,
                filename=filename,
            )
            df = DatasetFile(
                dataset_id=dataset.id,
                file_type=FileType(file_type_str),
                file_path=str(saved_path),
            )
            self.db.add(df)

        self.db.commit()
        self.db.refresh(dataset)
        return dataset

    def list_datasets_for_owner(self, owner_user_id: str) -> list[Dataset]:
        """オーナーの全データセット一覧"""
        owner = self._get_user_or_raise(owner_user_id)
        return list(
            self.db.execute(
                select(Dataset).where(Dataset.owner_user_id == owner.id)
            ).scalars().all()
        )

    def list_published_datasets(self) -> list[Dataset]:
        """公開済みデータセット一覧"""
        return list(
            self.db.execute(
                select(Dataset).where(Dataset.is_published.is_(True))
            ).scalars().all()
        )

    def get_dataset(self, dataset_id: str, actor_user_id: str) -> Dataset:
        """データセット取得。オーナーまたは公開済みなら閲覧可、それ以外はPermissionError"""
        actor = self._get_user_or_raise(actor_user_id)
        dataset = self.db.execute(
            select(Dataset).where(Dataset.dataset_id == dataset_id)
        ).scalar_one_or_none()
        if dataset is None:
            raise ValueError(f"Dataset not found: {dataset_id}")

        if dataset.owner_user_id == actor.id or dataset.is_published:
            return dataset

        raise PermissionError(
            f"User '{actor_user_id}' has no access to dataset '{dataset_id}'"
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _get_user_or_raise(self, user_id: str) -> User:
        user = self.db.execute(
            select(User).where(User.user_id == user_id)
        ).scalar_one_or_none()
        if user is None:
            raise ValueError(f"User not found: {user_id}")
        return user

    def _next_dataset_id(self) -> str:
        max_seq = self.db.execute(
            select(func.max(Dataset.id))
        ).scalar() or 0
        return generate_id("DS", max_seq + 1)
