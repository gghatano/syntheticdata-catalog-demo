from __future__ import annotations

import json

import typer

app = typer.Typer(help="カタログ管理")


def _init_db():
    from app.db.base import Base
    from app.db.session import SessionLocal, engine

    Base.metadata.create_all(bind=engine)
    return SessionLocal()


@app.command()
def derive(
    dataset_id: str = typer.Option(..., help="データセットID"),
    user: str = typer.Option(..., help="実行者のuser_id (hrロール)"),
    output_json: bool = typer.Option(False, "--json", help="JSON形式で出力"),
):
    """カタログ案を自動生成"""
    db = _init_db()
    try:
        from app.services.catalog_service import CatalogService
        from app.storage.file_store import FileStore

        file_store = FileStore()
        svc = CatalogService(db, file_store)
        columns = svc.derive_catalog(dataset_id, user)

        if output_json:
            data = [
                {
                    "column_name": c.column_name,
                    "inferred_type": c.inferred_type,
                    "description": c.description,
                    "is_pii": c.is_pii,
                    "pii_reason": c.pii_reason,
                    "stats": json.loads(c.stats_json),
                }
                for c in columns
            ]
            typer.echo(json.dumps(data, ensure_ascii=False, indent=2))
        else:
            typer.echo(f"カタログを生成しました (dataset={dataset_id}, {len(columns)}列):")
            typer.echo(f"{'列名':20s} {'型':8s} {'PII':5s} {'理由'}")
            typer.echo("-" * 70)
            for c in columns:
                pii = "Yes" if c.is_pii else "No"
                reason = c.pii_reason or ""
                typer.echo(f"{c.column_name:20s} {c.inferred_type:8s} {pii:5s} {reason}")
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
    """カタログを表示"""
    db = _init_db()
    try:
        from app.services.catalog_service import CatalogService
        from app.storage.file_store import FileStore

        file_store = FileStore()
        svc = CatalogService(db, file_store)
        columns = svc.get_catalog(dataset_id, user)

        if output_json:
            data = [
                {
                    "column_name": c.column_name,
                    "inferred_type": c.inferred_type,
                    "description": c.description,
                    "is_pii": c.is_pii,
                    "pii_reason": c.pii_reason,
                    "stats": json.loads(c.stats_json),
                }
                for c in columns
            ]
            typer.echo(json.dumps(data, ensure_ascii=False, indent=2))
        else:
            if not columns:
                typer.echo("カタログが見つかりません。")
                return
            typer.echo(f"カタログ (dataset={dataset_id}, {len(columns)}列):")
            typer.echo(f"{'列名':20s} {'型':8s} {'PII':5s} {'説明'}")
            typer.echo("-" * 70)
            for c in columns:
                pii = "Yes" if c.is_pii else "No"
                typer.echo(f"{c.column_name:20s} {c.inferred_type:8s} {pii:5s} {c.description}")
    except (ValueError, PermissionError) as e:
        typer.echo(f"エラー: {e}", err=True)
        raise typer.Exit(code=1)
    finally:
        db.close()


@app.command()
def update(
    dataset_id: str = typer.Option(..., help="データセットID"),
    user: str = typer.Option(..., help="実行者のuser_id (hrロール)"),
    column: str = typer.Option(..., help="更新対象の列名"),
    pii: bool = typer.Option(None, help="PIIフラグ (true/false)"),
    description: str = typer.Option(None, help="説明テキスト"),
    output_json: bool = typer.Option(False, "--json", help="JSON形式で出力"),
):
    """カタログ列を更新（PIIフラグ、説明など）"""
    db = _init_db()
    try:
        from app.services.catalog_service import CatalogService
        from app.storage.file_store import FileStore

        file_store = FileStore()
        svc = CatalogService(db, file_store)

        update_data: dict = {"column_name": column}
        if pii is not None:
            update_data["is_pii"] = pii
        if description is not None:
            update_data["description"] = description

        columns = svc.update_catalog(dataset_id, [update_data], user)
        updated_col = next((c for c in columns if c.column_name == column), None)

        if output_json:
            if updated_col:
                typer.echo(json.dumps({
                    "column_name": updated_col.column_name,
                    "is_pii": updated_col.is_pii,
                    "description": updated_col.description,
                }, ensure_ascii=False, indent=2))
        else:
            if updated_col:
                pii_str = "Yes" if updated_col.is_pii else "No"
                typer.echo(f"列 '{column}' を更新しました: PII={pii_str}, 説明={updated_col.description}")
            else:
                typer.echo(f"列 '{column}' が見つかりません。")
    except (ValueError, PermissionError) as e:
        typer.echo(f"エラー: {e}", err=True)
        raise typer.Exit(code=1)
    finally:
        db.close()
