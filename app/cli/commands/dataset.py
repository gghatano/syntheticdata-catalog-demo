from __future__ import annotations

import json
from pathlib import Path

import typer

app = typer.Typer(help="データセット管理")


def _init_db():
    from app.db.base import Base
    from app.db.session import SessionLocal, engine

    Base.metadata.create_all(bind=engine)
    return SessionLocal()


@app.command()
def create(
    owner: str = typer.Option(..., help="オーナーのuser_id (hrロール)"),
    name: str = typer.Option(..., help="データセット名"),
    employee_master: Path = typer.Option(..., help="employee_master CSVファイルパス"),
    project_allocation: Path = typer.Option(..., help="project_allocation CSVファイルパス"),
    working_hours: Path = typer.Option(..., help="working_hours CSVファイルパス"),
    output_json: bool = typer.Option(False, "--json", help="JSON形式で出力"),
):
    """データセットを作成"""
    db = _init_db()
    try:
        from app.services.dataset_service import DatasetService
        from app.storage.file_store import FileStore

        file_store = FileStore()
        svc = DatasetService(db, file_store)

        files = {
            "employee_master": employee_master.read_bytes(),
            "project_allocation": project_allocation.read_bytes(),
            "working_hours": working_hours.read_bytes(),
        }
        ds = svc.create_dataset(owner, name, files)

        if output_json:
            typer.echo(json.dumps({
                "dataset_id": ds.dataset_id,
                "name": ds.name,
                "is_published": ds.is_published,
            }, ensure_ascii=False, indent=2))
        else:
            typer.echo(f"データセットを作成しました: {ds.dataset_id} ({ds.name})")
    except (ValueError, PermissionError) as e:
        typer.echo(f"エラー: {e}", err=True)
        raise typer.Exit(code=1)
    finally:
        db.close()


@app.command("list")
def list_datasets(
    owner: str = typer.Option(None, help="オーナーのuser_id (省略時は全公開データセット)"),
    output_json: bool = typer.Option(False, "--json", help="JSON形式で出力"),
):
    """データセット一覧を表示"""
    db = _init_db()
    try:
        from app.services.dataset_service import DatasetService
        from app.storage.file_store import FileStore

        file_store = FileStore()
        svc = DatasetService(db, file_store)

        if owner:
            datasets = svc.list_datasets_for_owner(owner)
        else:
            datasets = svc.list_published_datasets()

        if output_json:
            data = [
                {
                    "dataset_id": ds.dataset_id,
                    "name": ds.name,
                    "is_published": ds.is_published,
                }
                for ds in datasets
            ]
            typer.echo(json.dumps(data, ensure_ascii=False, indent=2))
        else:
            if not datasets:
                typer.echo("データセットが見つかりません。")
                return
            typer.echo(f"{'ID':12s} {'名前':20s} {'公開':6s}")
            typer.echo("-" * 40)
            for ds in datasets:
                pub = "Yes" if ds.is_published else "No"
                typer.echo(f"{ds.dataset_id:12s} {ds.name:20s} {pub:6s}")
    except (ValueError, PermissionError) as e:
        typer.echo(f"エラー: {e}", err=True)
        raise typer.Exit(code=1)
    finally:
        db.close()


@app.command()
def show(
    dataset_id: str = typer.Option(..., help="データセットID"),
    user: str = typer.Option(..., help="閲覧者のuser_id"),
    output_json: bool = typer.Option(False, "--json", help="JSON形式で出力"),
):
    """データセット詳細を表示"""
    db = _init_db()
    try:
        from app.services.dataset_service import DatasetService
        from app.storage.file_store import FileStore

        file_store = FileStore()
        svc = DatasetService(db, file_store)
        ds = svc.get_dataset(dataset_id, user)

        if output_json:
            data = {
                "dataset_id": ds.dataset_id,
                "name": ds.name,
                "is_published": ds.is_published,
                "files": [
                    {"file_type": f.file_type.value, "file_path": f.file_path}
                    for f in ds.files
                ],
            }
            typer.echo(json.dumps(data, ensure_ascii=False, indent=2))
        else:
            typer.echo(f"データセットID: {ds.dataset_id}")
            typer.echo(f"名前: {ds.name}")
            typer.echo(f"公開: {'Yes' if ds.is_published else 'No'}")
            typer.echo("ファイル:")
            for f in ds.files:
                typer.echo(f"  {f.file_type.value}: {f.file_path}")
    except (ValueError, PermissionError) as e:
        typer.echo(f"エラー: {e}", err=True)
        raise typer.Exit(code=1)
    finally:
        db.close()
