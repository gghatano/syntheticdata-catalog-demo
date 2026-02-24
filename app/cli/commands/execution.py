from __future__ import annotations

import json

import typer

app = typer.Typer(help="実行管理")


def _init_db():
    from app.db.base import Base
    from app.db.session import SessionLocal, engine

    Base.metadata.create_all(bind=engine)
    return SessionLocal()


@app.command()
def run(
    submission_id: str = typer.Option(..., help="提出物ID"),
    mode: str = typer.Option("synthetic", help="実行モード (synthetic / real)"),
    executor: str = typer.Option(..., help="実行者のuser_id (hrロール)"),
    output_json: bool = typer.Option(False, "--json", help="JSON形式で出力"),
):
    """提出物を実行"""
    db = _init_db()
    try:
        from app.services.execution_service import ExecutionService
        from app.storage.file_store import FileStore

        file_store = FileStore()
        svc = ExecutionService(db, file_store)
        execution = svc.run_submission(submission_id, executor, mode)

        if output_json:
            typer.echo(json.dumps({
                "execution_id": execution.execution_id,
                "submission_id": submission_id,
                "mode": execution.mode.value,
                "status": execution.status.value,
            }, ensure_ascii=False, indent=2))
        else:
            typer.echo(f"実行しました: {execution.execution_id}")
            typer.echo(f"  モード: {execution.mode.value}")
            typer.echo(f"  ステータス: {execution.status.value}")
    except (ValueError, PermissionError) as e:
        typer.echo(f"エラー: {e}", err=True)
        raise typer.Exit(code=1)
    finally:
        db.close()


@app.command()
def show(
    execution_id: str = typer.Option(..., help="実行ID"),
    user: str = typer.Option(..., help="閲覧者のuser_id"),
    output_json: bool = typer.Option(False, "--json", help="JSON形式で出力"),
):
    """実行詳細を表示"""
    db = _init_db()
    try:
        from app.services.execution_service import ExecutionService
        from app.storage.file_store import FileStore

        file_store = FileStore()
        svc = ExecutionService(db, file_store)
        execution = svc.get_execution(execution_id, user)

        if output_json:
            typer.echo(json.dumps({
                "execution_id": execution.execution_id,
                "mode": execution.mode.value,
                "status": execution.status.value,
                "stdout_path": execution.stdout_path,
                "stderr_path": execution.stderr_path,
                "output_path": execution.output_path,
            }, ensure_ascii=False, indent=2))
        else:
            typer.echo(f"実行ID: {execution.execution_id}")
            typer.echo(f"モード: {execution.mode.value}")
            typer.echo(f"ステータス: {execution.status.value}")
            if execution.stdout_path:
                typer.echo(f"stdout: {execution.stdout_path}")
            if execution.stderr_path:
                typer.echo(f"stderr: {execution.stderr_path}")
            if execution.output_path:
                typer.echo(f"出力: {execution.output_path}")
    except (ValueError, PermissionError) as e:
        typer.echo(f"エラー: {e}", err=True)
        raise typer.Exit(code=1)
    finally:
        db.close()


@app.command("publish-result")
def publish_result(
    execution_id: str = typer.Option(..., help="実行ID"),
    scope: str = typer.Option("submitter", help="公開範囲 (submitter / public)"),
    user: str = typer.Option(..., help="実行者のuser_id (hrロール)"),
    output_json: bool = typer.Option(False, "--json", help="JSON形式で出力"),
):
    """実行結果を公開"""
    db = _init_db()
    try:
        from app.services.result_service import ResultService

        svc = ResultService(db)
        result = svc.publish_result(execution_id, user, scope)

        if output_json:
            typer.echo(json.dumps({
                "execution_id": execution_id,
                "scope": result.scope.value,
                "published_at": str(result.published_at),
            }, ensure_ascii=False, indent=2))
        else:
            typer.echo(f"実行結果 {execution_id} を scope={result.scope.value} で公開しました。")
    except (ValueError, PermissionError) as e:
        typer.echo(f"エラー: {e}", err=True)
        raise typer.Exit(code=1)
    finally:
        db.close()
