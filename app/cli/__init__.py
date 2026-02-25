import typer

from app.cli.commands import catalog, dataset, demo, execution, proposal, submission, synthetic, users

app = typer.Typer(name="app-cli", help="合成データ活用デモ基盤 CLI")
app.add_typer(users.app, name="users")
app.add_typer(dataset.app, name="dataset")
app.add_typer(catalog.app, name="catalog")
app.add_typer(synthetic.app, name="synthetic")
app.add_typer(proposal.app, name="proposal")
app.add_typer(submission.app, name="submission")
app.add_typer(execution.app, name="execution")
app.add_typer(demo.app, name="demo")

if __name__ == "__main__":
    app()
