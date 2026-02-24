from __future__ import annotations

import json

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models import Dataset, DatasetFile, FileType, SyntheticArtifact, User, UserRole
from app.storage.file_store import FileStore
from app.synthetic.generator import SyntheticGenerator
from app.synthetic.quality_report import QualityReporter


class SyntheticService:
    def __init__(self, db: Session, file_store: FileStore):
        self.db = db
        self.file_store = file_store

    def generate(
        self, dataset_id: str, actor_user_id: str, seed: int = 42
    ) -> list[SyntheticArtifact]:
        """
        1. データセット取得・権限チェック（hrのみ）
        2. 実データCSVをFileStoreから読み込み
        3. SyntheticGeneratorで合成データ生成
        4. FileStoreで合成データ保存
        5. QualityReporterで品質レポート生成・保存
        6. SyntheticArtifactレコード作成
        7. commit & return
        """
        actor = self._get_user_or_raise(actor_user_id)
        if actor.role != UserRole.hr:
            raise PermissionError("Only hr users can generate synthetic data")

        dataset = self._get_dataset_or_raise(dataset_id)

        # Read real data CSVs
        real_data_dir = self.file_store.get_real_data_path(dataset_id)
        generator = SyntheticGenerator()
        synthetic_dfs = generator.generate_all(real_data_dir, seed=seed)

        # Read original DataFrames for quality report
        originals = {}
        for key in synthetic_dfs:
            csv_path = real_data_dir / f"{key}.csv"
            if csv_path.exists():
                originals[key] = self.file_store.read_csv(csv_path)

        # Quality report
        reporter = QualityReporter()
        report = reporter.generate_full_report(originals, synthetic_dfs)
        report_path = self.file_store.save_quality_report(dataset_id, report)

        # Save synthetic data and create artifact records
        artifacts: list[SyntheticArtifact] = []
        for file_type_str, syn_df in synthetic_dfs.items():
            saved_path = self.file_store.save_synthetic_data(
                dataset_id=dataset_id,
                file_type=file_type_str,
                df=syn_df,
            )
            artifact = SyntheticArtifact(
                dataset_id=dataset.id,
                seed=seed,
                file_type=FileType(file_type_str),
                file_path=str(saved_path),
                quality_report_path=str(report_path),
            )
            self.db.add(artifact)
            artifacts.append(artifact)

        self.db.commit()
        for a in artifacts:
            self.db.refresh(a)
        return artifacts

    def get_quality_report(self, dataset_id: str, actor_user_id: str) -> dict:
        """品質レポート取得"""
        actor = self._get_user_or_raise(actor_user_id)
        dataset = self._get_dataset_or_raise(dataset_id)

        # Check access: owner or published
        if dataset.owner_user_id != actor.id and not dataset.is_published:
            raise PermissionError(
                f"User '{actor_user_id}' has no access to dataset '{dataset_id}'"
            )

        artifact = self.db.execute(
            select(SyntheticArtifact).where(SyntheticArtifact.dataset_id == dataset.id)
        ).scalars().first()
        if artifact is None or artifact.quality_report_path is None:
            raise ValueError(f"No quality report found for dataset: {dataset_id}")

        from pathlib import Path
        report_file = Path(artifact.quality_report_path)
        if not report_file.exists():
            raise ValueError(f"Quality report file not found: {artifact.quality_report_path}")

        return json.loads(report_file.read_text(encoding="utf-8"))

    def publish(self, dataset_id: str, actor_user_id: str, is_public: bool) -> Dataset:
        """データセットの公開/非公開切り替え（hrのみ）"""
        actor = self._get_user_or_raise(actor_user_id)
        if actor.role != UserRole.hr:
            raise PermissionError("Only hr users can publish datasets")

        dataset = self._get_dataset_or_raise(dataset_id)
        if dataset.owner_user_id != actor.id:
            raise PermissionError(
                f"User '{actor_user_id}' is not the owner of dataset '{dataset_id}'"
            )

        dataset.is_published = is_public
        self.db.commit()
        self.db.refresh(dataset)
        return dataset

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
