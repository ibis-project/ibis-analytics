import os
import httpx
import typer
import subprocess

from ibis_analytics.catalog import delta_table_path
from ibis_analytics.etl.run import main as etl_main
from ibis_analytics.ingest.run import main as ingest_main

from ibis_analytics.config import (
    DATA_DIR,
    RAW_DATA_DIR,
    GH_PRS_TABLE,
    GH_FORKS_TABLE,
    GH_STARS_TABLE,
    GH_ISSUES_TABLE,
    GH_COMMITS_TABLE,
    GH_WATCHERS_TABLE,
    DOCS_TABLE,
    ZULIP_MEMBERS_TABLE,
    ZULIP_MESSAGES_TABLE,
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
def ingest(
    gh: bool = typer.Option(True, "--gh", help="Ingest GitHub data", show_default=True),
    zulip: bool = typer.Option(
        True, "--zulip", help="Ingest Zulip data", show_default=True
    ),
    docs: bool = typer.Option(
        False, "--docs", help="Ingest docs data", show_default=True
    ),
):
    """Ingest source data."""
    # ensure project config exists
    try:
        ingest_main(gh=gh, zulip=zulip, docs=docs)
    except KeyboardInterrupt:
        typer.echo("stopping...")

    except Exception as e:
        typer.echo(f"error: {e}")


@app.command()
@app.command("etl", hidden=True)
def run(
    gh: bool = typer.Option(True, "--gh", help="Run GitHub ETL", show_default=True),
    zulip: bool = typer.Option(
        True, "--zulip", help="Run Zulip ETL", show_default=True
    ),
    docs: bool = typer.Option(False, "--docs", help="Run docs ETL", show_default=True),
):
    """Run ETL."""

    try:
        etl_main(gh=gh, zulip=zulip, docs=docs)
    except KeyboardInterrupt:
        typer.echo("stopping...")
    except Exception as e:
        typer.echo(f"error: {e}")


@app.command()
@app.command("app", hidden=True)
@app.command("dash", hidden=True)
@app.command("metrics", hidden=True)
def dashboard():
    """Open the dashboard."""

    if not os.path.exists("dashboard.py"):
        url = "https://raw.githubusercontent.com/ibis-project/ibis-analytics/main/dashboard.py"

        response = httpx.get(url)
        if response.status_code != 200:
            typer.echo(f"error: {response.text}")
            return
        dashboard_code = response.text

        typer.echo("creating dashboard.py...")
        with open("dashboard.py", "w") as f:
            f.write(dashboard_code)
    else:
        typer.echo("found dashboard.py")

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
        DOCS_TABLE,
        ZULIP_MEMBERS_TABLE,
        ZULIP_MESSAGES_TABLE,
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
