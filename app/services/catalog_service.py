from __future__ import annotations

from pathlib import Path

import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.catalog.catalog_generator import CatalogGenerator
from app.db.models import CatalogColumn, Dataset, User, UserRole
from app.storage.file_store import FileStore


class CatalogService:
    def __init__(self, db: Session, file_store: FileStore):
        self.db = db
        self.file_store = file_store

    def derive_catalog(self, dataset_id: str, actor_user_id: str) -> list[CatalogColumn]:
        """Generate catalog columns from CSV files and save to DB.

        Only hr role users can derive catalogs.
        """
        actor = self._get_user_or_raise(actor_user_id)
        if actor.role != UserRole.hr:
            raise PermissionError(
                f"Only hr users can derive catalogs. User '{actor_user_id}' has role '{actor.role.value}'"
            )

        dataset = self._get_dataset_or_raise(dataset_id)

        # Delete existing catalog columns for this dataset
        existing = self.db.execute(
            select(CatalogColumn).where(CatalogColumn.dataset_id == dataset.id)
        ).scalars().all()
        for col in existing:
            self.db.delete(col)
        self.db.flush()

        # Read all CSV files for this dataset and generate catalog
        generator = CatalogGenerator()
        all_columns: list[CatalogColumn] = []

        for dataset_file in dataset.files:
            file_path = Path(dataset_file.file_path)
            if not file_path.exists():
                continue
            df = pd.read_csv(file_path)
            col_infos = generator.generate(df)

            for info in col_infos:
                catalog_col = CatalogColumn(
                    dataset_id=dataset.id,
                    column_name=info.column_name,
                    inferred_type=info.inferred_type,
                    description=info.description,
                    is_pii=info.is_pii,
                    pii_reason=info.pii_reason,
                    stats_json=info.stats_json,
                )
                self.db.add(catalog_col)
                all_columns.append(catalog_col)

        self.db.commit()
        for col in all_columns:
            self.db.refresh(col)
        return all_columns

    def update_catalog(
        self,
        dataset_id: str,
        columns_update: list[dict],
        actor_user_id: str,
    ) -> list[CatalogColumn]:
        """Update catalog columns (e.g. PII flags, descriptions).

        Only hr role users can update catalogs.
        columns_update: list of dicts with 'column_name' and optional 'is_pii', 'description'.
        """
        actor = self._get_user_or_raise(actor_user_id)
        if actor.role != UserRole.hr:
            raise PermissionError(
                f"Only hr users can update catalogs. User '{actor_user_id}' has role '{actor.role.value}'"
            )

        dataset = self._get_dataset_or_raise(dataset_id)

        existing = {
            col.column_name: col
            for col in self.db.execute(
                select(CatalogColumn).where(CatalogColumn.dataset_id == dataset.id)
            ).scalars().all()
        }

        for update in columns_update:
            col_name = update["column_name"]
            if col_name not in existing:
                raise ValueError(f"Column not found in catalog: {col_name}")
            catalog_col = existing[col_name]
            if "is_pii" in update:
                catalog_col.is_pii = update["is_pii"]
                if not update["is_pii"]:
                    catalog_col.pii_reason = None
            if "description" in update:
                catalog_col.description = update["description"]

        self.db.commit()
        return self.get_catalog(dataset_id, actor_user_id)

    def get_catalog(self, dataset_id: str, actor_user_id: str) -> list[CatalogColumn]:
        """Get catalog columns for a dataset.

        HR users can always access. Other users can access published datasets.
        """
        actor = self._get_user_or_raise(actor_user_id)
        dataset = self._get_dataset_or_raise(dataset_id)

        if actor.role != UserRole.hr and not dataset.is_published:
            raise PermissionError(
                f"User '{actor_user_id}' has no access to catalog for dataset '{dataset_id}'"
            )

        return list(
            self.db.execute(
                select(CatalogColumn).where(CatalogColumn.dataset_id == dataset.id)
            ).scalars().all()
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

    def _get_dataset_or_raise(self, dataset_id: str) -> Dataset:
        dataset = self.db.execute(
            select(Dataset).where(Dataset.dataset_id == dataset_id)
        ).scalar_one_or_none()
        if dataset is None:
            raise ValueError(f"Dataset not found: {dataset_id}")
        return dataset
