import os
import typer
import httpx
import subprocess

from ibis_analytics.etl import main as etl_main
from ibis_analytics.ingest import main as ingest_main
from ibis_analytics.catalog import delta_table_path

from ibis_analytics.config import (
    DATA_DIR,
    RAW_DATA_DIR,
    GH_PRS_TABLE,
    GH_FORKS_TABLE,
    GH_STARS_TABLE,
    GH_ISSUES_TABLE,
    GH_COMMITS_TABLE,
    GH_WATCHERS_TABLE,
)

TYPER_KWARGS = {
    "no_args_is_help": True,
    "add_completion": False,
    "context_settings": {"help_option_names": ["-h", "--help"]},
}
app = typer.Typer(help="ia", **TYPER_KWARGS)
clean_app = typer.Typer(help="Clean the data lake.", **TYPER_KWARGS)

## add subcommands
app.add_typer(clean_app, name="clean")

## add subcommand aliases
app.add_typer(clean_app, name="c", hidden=True)


# commands
@app.command()
def ingest():
    """Ingest source data."""
    # ensure project config exists
    try:
        ingest_main()
    except KeyboardInterrupt:
        typer.echo("stopping...")

    except Exception as e:
        typer.echo(f"error: {e}")


@app.command()
@app.command("etl", hidden=True)
def run():
    """Run ETL."""

    try:
        etl_main()
    except KeyboardInterrupt:
        typer.echo("stopping...")
    except Exception as e:
        typer.echo(f"error: {e}")


@app.command()
@app.command("dash", hidden=True)
@app.command("metrics", hidden=True)
def dashboard():
    """Open the dashboard."""

    typer.echo("opening dashboard...")
    cmd = "shiny run dashboard.py -b"
    typer.echo(f"running: {cmd}...")
    subprocess.call(cmd, shell=True)


@clean_app.command("lake")
def clean_lake():
    """Clean the data lake."""
    tables = [
        GH_PRS_TABLE,
        GH_FORKS_TABLE,
        GH_STARS_TABLE,
        GH_ISSUES_TABLE,
        GH_COMMITS_TABLE,
        GH_WATCHERS_TABLE,
    ]

    for table in tables:
        cmd = f"rm -rf {delta_table_path(table)}/"
        typer.echo(f"running: {cmd}...")
        subprocess.call(cmd, shell=True)


@clean_app.command("ingest")
def clean_ingest(
    confirm: bool = typer.Option(
        True, "--confirm", "-c", help="Confirm deletion", show_default=True
    ),
):
    """Clean the raw data."""
    # ensure the data ingested exists
    if confirm:
        typer.confirm("Are you sure you want to delete the ingested data?", abort=True)

    cmd = f"rm -rf {os.path.join(DATA_DIR, RAW_DATA_DIR)}/"
    typer.echo(f"running: {cmd}...")
    subprocess.call(cmd, shell=True)


@clean_app.command("all")
def clean_all():
    """Clean all the data."""
    clean_lake()
    clean_ingest()


if __name__ == "__main__":
    typer.run(app)