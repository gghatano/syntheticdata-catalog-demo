from __future__ import annotations

import json
from pathlib import Path

import typer

app = typer.Typer(help="提出物管理")


def _init_db():
    from app.db.base import Base
    from app.db.session import SessionLocal, engine

    Base.metadata.create_all(bind=engine)
    return SessionLocal()


@app.command()
def create(
    user: str = typer.Option(..., help="提出者のuser_id"),
    dataset_id: str = typer.Option(..., help="対象データセットID"),
    title: str = typer.Option(..., help="提出タイトル"),
    description: str = typer.Option("", help="説明"),
    zip_path: Path = typer.Option(..., "--zip", help="提出ZIPファイルパス"),
    output_json: bool = typer.Option(False, "--json", help="JSON形式で出力"),
):
    """提出物を作成"""
    db = _init_db()
    try:
        from app.services.submission_service import SubmissionService
        from app.storage.file_store import FileStore

        file_store = FileStore()
        svc = SubmissionService(db, file_store)

        zip_content = zip_path.read_bytes()
        sub = svc.create_submission(user, dataset_id, title, description, zip_content)

        if output_json:
            typer.echo(json.dumps({
                "submission_id": sub.submission_id,
                "dataset_id": dataset_id,
                "title": sub.title,
                "status": sub.status.value,
            }, ensure_ascii=False, indent=2))
        else:
            typer.echo(f"提出物を作成しました: {sub.submission_id}")
            typer.echo(f"  タイトル: {sub.title}")
            typer.echo(f"  ステータス: {sub.status.value}")
    except (ValueError, PermissionError) as e:
        typer.echo(f"エラー: {e}", err=True)
        raise typer.Exit(code=1)
    finally:
        db.close()


@app.command("list")
def list_submissions(
    dataset_id: str = typer.Option(..., help="データセットID"),
    user: str = typer.Option(..., help="閲覧者のuser_id"),
    output_json: bool = typer.Option(False, "--json", help="JSON形式で出力"),
):
    """提出物一覧を表示"""
    db = _init_db()
    try:
        from app.services.submission_service import SubmissionService
        from app.storage.file_store import FileStore

        file_store = FileStore()
        svc = SubmissionService(db, file_store)
        subs = svc.list_submissions(dataset_id, user)

        if output_json:
            data = [
                {
                    "submission_id": s.submission_id,
                    "title": s.title,
                    "status": s.status.value,
                }
                for s in subs
            ]
            typer.echo(json.dumps(data, ensure_ascii=False, indent=2))
        else:
            if not subs:
                typer.echo("提出物が見つかりません。")
                return
            typer.echo(f"{'ID':12s} {'タイトル':30s} {'ステータス':20s}")
            typer.echo("-" * 65)
            for s in subs:
                typer.echo(f"{s.submission_id:12s} {s.title:30s} {s.status.value:20s}")
    except (ValueError, PermissionError) as e:
        typer.echo(f"エラー: {e}", err=True)
        raise typer.Exit(code=1)
    finally:
        db.close()


@app.command()
def approve(
    submission_id: str = typer.Option(..., help="提出物ID"),
    approver: str = typer.Option(..., help="承認者のuser_id (hrロール)"),
    output_json: bool = typer.Option(False, "--json", help="JSON形式で出力"),
):
    """提出物を承認"""
    db = _init_db()
    try:
        from app.services.submission_service import SubmissionService
        from app.storage.file_store import FileStore

        file_store = FileStore()
        svc = SubmissionService(db, file_store)
        sub = svc.approve_submission(submission_id, approver)

        if output_json:
            typer.echo(json.dumps({
                "submission_id": sub.submission_id,
                "status": sub.status.value,
            }, ensure_ascii=False, indent=2))
        else:
            typer.echo(f"提出物 {sub.submission_id} を承認しました。")
    except (ValueError, PermissionError) as e:
        typer.echo(f"エラー: {e}", err=True)
        raise typer.Exit(code=1)
    finally:
        db.close()


@app.command()
def reject(
    submission_id: str = typer.Option(..., help="提出物ID"),
    approver: str = typer.Option(..., help="承認者のuser_id (hrロール)"),
    reason: str = typer.Option(..., help="却下理由"),
    output_json: bool = typer.Option(False, "--json", help="JSON形式で出力"),
):
    """提出物を却下"""
    db = _init_db()
    try:
        from app.services.submission_service import SubmissionService
        from app.storage.file_store import FileStore

        file_store = FileStore()
        svc = SubmissionService(db, file_store)
        sub = svc.reject_submission(submission_id, approver, reason)

        if output_json:
            typer.echo(json.dumps({
                "submission_id": sub.submission_id,
                "status": sub.status.value,
            }, ensure_ascii=False, indent=2))
        else:
            typer.echo(f"提出物 {sub.submission_id} を却下しました。")
    except (ValueError, PermissionError) as e:
        typer.echo(f"エラー: {e}", err=True)
        raise typer.Exit(code=1)
    finally:
        db.close()
