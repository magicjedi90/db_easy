# db_easy/cli.py
import click
from pathlib import Path

from .config import load_config
from .executor import sync_database


@click.group()
def cli():
    """db-easy command-line interface."""
    pass


@cli.command()
@click.option(
    "--project",
    "-p",
    "project_path",
    default=".",
    type=click.Path(file_okay=False, dir_okay=True),
    help="Path to schema repo containing db-easy.yaml & schema/",
)
@click.option(
    "--db",
    "db_url",
    default=None,
    help="Database URL to override the config file / env var",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Parse & list SQL without executing anything",
)
@click.option(
    "--vars",
    "var_kv",
    default=None,
    help="Comma-separated key=value pairs for Jinja template vars",
)
def sync(project_path, db_url, dry_run, var_kv):
    """Synchronize database with any pending steps."""
    cfg = load_config(Path(project_path), db_url_override=db_url, cli_vars=var_kv)
    sync_database(cfg, dry_run=dry_run)


def run_cli() -> None:
    cli()
