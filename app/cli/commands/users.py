from __future__ import annotations

import json

import typer

app = typer.Typer(help="ユーザー管理")


def _init_db():
    from app.db.base import Base
    from app.db.session import SessionLocal, engine

    Base.metadata.create_all(bind=engine)
    return SessionLocal()


@app.command()
def seed(
    output_json: bool = typer.Option(False, "--json", help="JSON形式で出力"),
):
    """デモ用初期ユーザーを作成"""
    db = _init_db()
    try:
        from app.services.auth_service import AuthService

        svc = AuthService(db)
        users = svc.seed_users()
        if output_json:
            data = [
                {"user_id": u.user_id, "display_name": u.display_name, "role": u.role.value}
                for u in users
            ]
            typer.echo(json.dumps(data, ensure_ascii=False, indent=2))
        else:
            typer.echo("デモユーザーを作成しました:")
            for u in users:
                typer.echo(f"  {u.user_id:20s} {u.display_name:20s} role={u.role.value}")
    except (ValueError, PermissionError) as e:
        typer.echo(f"エラー: {e}", err=True)
        raise typer.Exit(code=1)
    finally:
        db.close()
