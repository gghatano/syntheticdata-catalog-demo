from __future__ import annotations

import json

import typer

app = typer.Typer(help="合成データ管理")


def _init_db():
    from app.db.base import Base
    from app.db.session import SessionLocal, engine

    Base.metadata.create_all(bind=engine)
    return SessionLocal()


@app.command()
def generate(
    dataset_id: str = typer.Option(..., help="データセットID"),
    user: str = typer.Option(..., help="実行者のuser_id (hrロール)"),
    seed: int = typer.Option(42, help="乱数シード"),
    output_json: bool = typer.Option(False, "--json", help="JSON形式で出力"),
):
    """合成データを生成"""
    db = _init_db()
    try:
        from app.services.synthetic_service import SyntheticService
        from app.storage.file_store import FileStore

        file_store = FileStore()
        svc = SyntheticService(db, file_store)
        artifacts = svc.generate(dataset_id, user, seed=seed)

        if output_json:
            data = [
                {
                    "file_type": a.file_type.value,
                    "file_path": a.file_path,
                    "seed": a.seed,
                    "quality_report_path": a.quality_report_path,
                }
                for a in artifacts
            ]
            typer.echo(json.dumps(data, ensure_ascii=False, indent=2))
        else:
            typer.echo(f"合成データを生成しました (dataset={dataset_id}, seed={seed}):")
            for a in artifacts:
                typer.echo(f"  {a.file_type.value}: {a.file_path}")
            if artifacts and artifacts[0].quality_report_path:
                typer.echo(f"品質レポート: {artifacts[0].quality_report_path}")
    except (ValueError, PermissionError) as e:
        typer.echo(f"エラー: {e}", err=True)
        raise typer.Exit(code=1)
    finally:
        db.close()


@app.command()
def publish(
    dataset_id: str = typer.Option(..., help="データセットID"),
    public: bool = typer.Option(True, help="公開/非公開"),
    user: str = typer.Option(..., help="実行者のuser_id (hrロール)"),
    output_json: bool = typer.Option(False, "--json", help="JSON形式で出力"),
):
    """データセットを公開/非公開に設定"""
    db = _init_db()
    try:
        from app.services.synthetic_service import SyntheticService
        from app.storage.file_store import FileStore

        file_store = FileStore()
        svc = SyntheticService(db, file_store)
        ds = svc.publish(dataset_id, user, public)

        if output_json:
            typer.echo(json.dumps({
                "dataset_id": ds.dataset_id,
                "name": ds.name,
                "is_published": ds.is_published,
            }, ensure_ascii=False, indent=2))
        else:
            status = "公開" if ds.is_published else "非公開"
            typer.echo(f"データセット {ds.dataset_id} を{status}に設定しました。")
    except (ValueError, PermissionError) as e:
        typer.echo(f"エラー: {e}", err=True)
        raise typer.Exit(code=1)
    finally:
        db.close()
