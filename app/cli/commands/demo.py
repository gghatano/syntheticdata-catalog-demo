from __future__ import annotations

from pathlib import Path

import typer

app = typer.Typer(help="デモ用一括操作")

EXAMPLES_DIR = Path(__file__).resolve().parent.parent.parent.parent / "examples" / "sample_data"


def _init_db():
    from app.db.base import Base
    from app.db.session import SessionLocal, engine

    Base.metadata.create_all(bind=engine)
    return SessionLocal()


@app.command("seed-data")
def seed_data():
    """デモ環境を一括セットアップ (ユーザー作成 + サンプルデータ登録 + 合成生成 + 公開)"""
    db = _init_db()
    try:
        from app.services.auth_service import AuthService
        from app.services.dataset_service import DatasetService
        from app.services.synthetic_service import SyntheticService
        from app.storage.file_store import FileStore

        file_store = FileStore()

        # 1. Seed users
        typer.echo("[1/4] デモユーザーを作成中...")
        auth_svc = AuthService(db)
        users = auth_svc.seed_users()
        for u in users:
            typer.echo(f"  {u.user_id} ({u.role.value})")

        # 2. Create sample dataset
        typer.echo("[2/4] サンプルデータセットを登録中...")
        dataset_svc = DatasetService(db, file_store)

        em_path = EXAMPLES_DIR / "employee_master.csv"
        pa_path = EXAMPLES_DIR / "project_allocation.csv"
        wh_path = EXAMPLES_DIR / "working_hours.csv"

        if not em_path.exists():
            typer.echo(f"エラー: サンプルデータが見つかりません: {em_path}", err=True)
            raise typer.Exit(code=1)

        files = {
            "employee_master": em_path.read_bytes(),
            "project_allocation": pa_path.read_bytes(),
            "working_hours": wh_path.read_bytes(),
        }

        try:
            ds = dataset_svc.create_dataset("hr_demo", "サンプル人事データ", files)
        except ValueError as e:
            if "already exists" in str(e).lower():
                typer.echo("  (データセット作成をスキップ - 既存のデータセットを使用)")
                existing = dataset_svc.list_datasets_for_owner("hr_demo")
                if not existing:
                    raise
                ds = existing[0]
            else:
                raise
        typer.echo(f"  データセット: {ds.dataset_id} ({ds.name})")

        # 3. Generate synthetic data
        typer.echo("[3/4] 合成データを生成中...")
        syn_svc = SyntheticService(db, file_store)
        try:
            artifacts = syn_svc.generate(ds.dataset_id, "hr_demo", seed=42)
            for a in artifacts:
                typer.echo(f"  {a.file_type.value}: {a.file_path}")
        except ValueError as e:
            typer.echo(f"  (スキップ: {e})")

        # 4. Publish dataset
        typer.echo("[4/4] データセットを公開中...")
        ds = syn_svc.publish(ds.dataset_id, "hr_demo", True)
        typer.echo(f"  {ds.dataset_id} -> 公開={ds.is_published}")

        typer.echo("\nデモ環境のセットアップが完了しました。")
    except (ValueError, PermissionError) as e:
        typer.echo(f"エラー: {e}", err=True)
        raise typer.Exit(code=1)
    finally:
        db.close()
