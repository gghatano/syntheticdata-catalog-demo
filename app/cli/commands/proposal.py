from __future__ import annotations

import json
from pathlib import Path

import typer

app = typer.Typer(help="提案管理")


def _init_db():
    from app.db.base import Base
    from app.db.session import SessionLocal, engine

    Base.metadata.create_all(bind=engine)
    return SessionLocal()


@app.command()
def create(
    user: str = typer.Option(..., help="提出者のuser_id"),
    dataset_id: str = typer.Option(..., help="対象データセットID"),
    title: str = typer.Option(..., help="提案タイトル"),
    summary: str = typer.Option("", help="提案概要"),
    code: Path = typer.Option(..., help="分析コードファイルパス (analysis.py)"),
    report: Path = typer.Option(..., help="レポートファイルパス (report.md)"),
    output_json: bool = typer.Option(False, "--json", help="JSON形式で出力"),
):
    """提案を作成"""
    db = _init_db()
    try:
        from app.services.proposal_service import ProposalService
        from app.storage.file_store import FileStore

        file_store = FileStore()
        svc = ProposalService(db, file_store)

        code_content = code.read_bytes()
        report_content = report.read_bytes()
        proposal = svc.create_proposal(
            actor_user_id=user,
            dataset_id=dataset_id,
            title=title,
            summary=summary,
            code_content=code_content,
            report_content=report_content,
        )

        if output_json:
            typer.echo(json.dumps({
                "proposal_id": proposal.proposal_id,
                "dataset_id": dataset_id,
                "title": proposal.title,
                "status": proposal.status.value,
            }, ensure_ascii=False, indent=2))
        else:
            typer.echo(f"提案を作成しました: {proposal.proposal_id}")
            typer.echo(f"  タイトル: {proposal.title}")
            typer.echo(f"  ステータス: {proposal.status.value}")
    except (ValueError, PermissionError) as e:
        typer.echo(f"エラー: {e}", err=True)
        raise typer.Exit(code=1)
    finally:
        db.close()


@app.command("list")
def list_proposals(
    dataset_id: str = typer.Option(..., help="データセットID"),
    user: str = typer.Option(..., help="閲覧者のuser_id"),
    output_json: bool = typer.Option(False, "--json", help="JSON形式で出力"),
):
    """提案一覧を表示"""
    db = _init_db()
    try:
        from app.services.proposal_service import ProposalService
        from app.storage.file_store import FileStore

        file_store = FileStore()
        svc = ProposalService(db, file_store)
        proposals = svc.list_proposals(dataset_id, user)

        if output_json:
            data = [
                {
                    "proposal_id": p.proposal_id,
                    "title": p.title,
                    "status": p.status.value,
                }
                for p in proposals
            ]
            typer.echo(json.dumps(data, ensure_ascii=False, indent=2))
        else:
            if not proposals:
                typer.echo("提案が見つかりません。")
                return
            typer.echo(f"{'ID':12s} {'タイトル':30s} {'ステータス':12s}")
            typer.echo("-" * 56)
            for p in proposals:
                typer.echo(f"{p.proposal_id:12s} {p.title:30s} {p.status.value:12s}")
    except (ValueError, PermissionError) as e:
        typer.echo(f"エラー: {e}", err=True)
        raise typer.Exit(code=1)
    finally:
        db.close()


@app.command()
def review(
    proposal_id: str = typer.Option(..., help="提案ID"),
    reviewer: str = typer.Option(..., help="レビュアーのuser_id (hrロール)"),
    action: str = typer.Option(..., help="アクション (approve/reject/comment)"),
    comment: str = typer.Option(..., help="コメント"),
    output_json: bool = typer.Option(False, "--json", help="JSON形式で出力"),
):
    """提案をレビュー（承認/却下/コメント）"""
    db = _init_db()
    try:
        from app.services.proposal_service import ProposalService
        from app.storage.file_store import FileStore

        file_store = FileStore()
        svc = ProposalService(db, file_store)
        review_comment = svc.review_proposal(proposal_id, reviewer, action, comment)

        if output_json:
            typer.echo(json.dumps({
                "proposal_id": proposal_id,
                "action": review_comment.action.value,
                "comment": review_comment.comment,
            }, ensure_ascii=False, indent=2))
        else:
            typer.echo(f"提案 {proposal_id} をレビューしました。")
            typer.echo(f"  アクション: {review_comment.action.value}")
            typer.echo(f"  コメント: {review_comment.comment}")
    except (ValueError, PermissionError) as e:
        typer.echo(f"エラー: {e}", err=True)
        raise typer.Exit(code=1)
    finally:
        db.close()


@app.command("run-actual")
def run_actual(
    proposal_id: str = typer.Option(..., help="提案ID"),
    executor: str = typer.Option(..., help="実行者のuser_id (hrロール)"),
    output_json: bool = typer.Option(False, "--json", help="JSON形式で出力"),
):
    """承認済み提案を実データで実行"""
    db = _init_db()
    try:
        from app.services.proposal_service import ProposalService
        from app.storage.file_store import FileStore

        file_store = FileStore()
        svc = ProposalService(db, file_store)

        proposal = svc.get_proposal(proposal_id, executor)
        if proposal.status.value != "approved":
            raise ValueError(f"Proposal {proposal_id} is not approved (status: {proposal.status.value})")

        if output_json:
            typer.echo(json.dumps({
                "proposal_id": proposal_id,
                "status": "execution_requested",
                "message": "Actual data execution is not yet implemented in this demo",
            }, ensure_ascii=False, indent=2))
        else:
            typer.echo(f"提案 {proposal_id} の実データ実行をリクエストしました。")
            typer.echo("  注: 実データ実行機能はデモ版では未実装です。")
    except (ValueError, PermissionError) as e:
        typer.echo(f"エラー: {e}", err=True)
        raise typer.Exit(code=1)
    finally:
        db.close()
