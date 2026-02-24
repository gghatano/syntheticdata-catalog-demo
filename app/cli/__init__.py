import typer

from app.cli.commands import dataset, demo, execution, submission, synthetic, users

app = typer.Typer(name="app-cli", help="合成データ活用デモ基盤 CLI")
app.add_typer(users.app, name="users")
app.add_typer(dataset.app, name="dataset")
app.add_typer(synthetic.app, name="synthetic")
app.add_typer(submission.app, name="submission")
app.add_typer(execution.app, name="execution")
app.add_typer(demo.app, name="demo")

if __name__ == "__main__":
    app()
