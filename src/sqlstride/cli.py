# sqlstride/cli.py
import click
from pathlib import Path

from .config import load_config
from .executor import sync_database, create_repository_structure


@click.group()
def cli():
    """sqlstride command-line interface."""
    pass


@cli.command()
@click.option(
    "--project",
    "-p",
    "project_path",
    default=".",
    type=click.Path(file_okay=False, dir_okay=True),
    help="Path to schema repo containing sqlstride.yaml & schema/",
)
@click.option(
    "--host",
    default=None,
    help="Database Port used for connecting",
)
@click.option(
    "--port",
    default=None,
    help="Database Port to connect to",
)
@click.option(
    "--instance",
    default=None,
    help="Instance used for connecting to MSSQL Database",
)
@click.option(
    "--database",
    "-db",
    default=None,
    help="Desired database to connect to on host",
)
@click.option(
    "--username",
    "-u",
    default=None,
    help="Username used for authenticating with the database",
)
@click.option(
    "--password",
    "-pw",
    default=None,
    help="Password used for authenticating with the database",
)
@click.option(
    "--trusted-auth",
    is_flag=True,
    default=False,
    help="Use trusted authentication for connecting to MSSQL Database",
)
@click.option(
    "--sql-dialect",
    default=None,
    help="SQL dialect to use for connecting to database"
)
@click.option(
    "--default-schema",
    default=None,
    help="Schema that the log and lock tables will be created in",
)
@click.option(
    "--log-table",
    default=None,
    help="Name of the table to use to keep track of changes"
)
@click.option(
    "--lock-table",
    default=None,
    help="Name of the table to use to lock the database during sync"
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Parse & list SQL without executing anything",
)
@click.option(
    "--same-checksums",
    is_flag=True,
    default=False,
    help="Checks the current checksums against the existing checksums and raises an error if they are different"
)
@click.option(
    "--jinja-vars",
    type=str,
    default=None,
    help="JSON string of variables to use in Jinja templates"
)
def sync(project_path, host, port, instance, database, username, password, trusted_auth,
                         sql_dialect, default_schema, log_table, lock_table, dry_run, same_checksums, jinja_vars):
    import json
    jinja_vars_dict = {}
    if jinja_vars:
        try:
            jinja_vars_dict = json.loads(jinja_vars)
        except json.JSONDecodeError:
            raise click.BadParameter("jinja-vars must be a valid JSON string")

    config = load_config(Path(project_path), host, port, instance, database, username, password, trusted_auth,
                         sql_dialect, default_schema, log_table, lock_table, jinja_vars_dict)
    sync_database(config, dry_run=dry_run, same_checksums=same_checksums)


@cli.command()
@click.option(
    "--project",
    "-p",
    "project_path",
    default=".",
    type=click.Path(file_okay=False, dir_okay=True),
    help="Path to create the repository structure",
)
def create_repo(project_path):
    """Create the repository structure with all required directories."""
    create_repository_structure(project_path)


def run_cli() -> None:
    cli()
