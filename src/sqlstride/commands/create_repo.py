# sqlstride/commands/create_repo.py
import os
from pathlib import Path

import click
from etl.logger import Logger

from ..constants import ORDERED_DIRS

logger = Logger().get_logger()


def create_repository_structure(project_path: str) -> None:
    """
    Create the repository structure with all directories from ORDERED_DIRS
    and a blank __init__.py file in each directory. Also creates a sqlstride.yaml
    file with user-provided configuration.

    Args:
        project_path: Path to the project directory
    """
    project_path = Path(project_path)

    # Get user input for the sqlstride.yaml file
    click.echo("Creating sqlstride.yaml configuration file...")

    # Required fields
    sql_dialect = click.prompt("SQL dialect", type=click.Choice(['postgres', 'mssql', 'mariadb']), default='postgres')
    host = click.prompt("Database host", default='localhost')

    # Optional fields
    port = click.prompt("Database port (optional, press Enter to skip)", default='', show_default=False)
    database = click.prompt("Database name (optional)", default='', show_default=False)
    instance = click.prompt("Database instance (for MSSQL, optional)", default='', show_default=False)
    trusted_auth = click.confirm("Use trusted authentication (for MSSQL)", default=False)
    username = click.prompt("Database username (optional)", default='', show_default=False)
    password = click.prompt("Database password (optional)", default='', show_default=False, hide_input=True)
    default_schema = click.prompt("Default schema (optional)", default='', show_default=False)
    log_table = click.prompt("Log table name", default='sqlstride_log')
    lock_table = click.prompt("Lock table name", default='sqlstride_lock')

    # Create the sqlstride.yaml file
    yaml_file = project_path / "sqlstride.yaml"
    with open(yaml_file, 'w') as file:
        file.write("# sqlstride Configuration\n\n")
        file.write(f"# Required\n")
        file.write(f"sql_dialect: {sql_dialect}\n")
        file.write(f"host: {host}\n\n")

        file.write(f"# Optional with no defaults\n")
        if port:
            file.write(f"port: {port}\n")
        if instance:
            file.write(f"instance: {instance}\n")
        if database:
            file.write(f"database: {database}\n")
        if username:
            file.write(f"username: {username}\n")
        if password:
            file.write(f"password: {password}\n")
        if default_schema:
            file.write(f"default_schema: {default_schema}\n")

        file.write(f"\n# Optional with defaults\n")
        file.write(f"trusted_auth: {str(trusted_auth).lower()}\n")
        file.write(f"log_table: {log_table}\n")
        file.write(f"lock_table: {lock_table}\n")
        file.write(f"\n# Jinja template variables\n")
        file.write(f"jinja_vars:\n")
        file.write(f"  # example_var: example_value\n")

    click.echo(f"Created sqlstride.yaml configuration file at {yaml_file}")

    # Create each directory and add __init__.py
    for directory in ORDERED_DIRS:
        dir_path = project_path / directory
        os.makedirs(dir_path, exist_ok=True)

    gitignore_path = project_path / ".gitignore"
    if gitignore_path.exists():
        with open(gitignore_path, "r+", encoding="utf-8") as gi:
            lines = [line.rstrip("\n") for line in gi.readlines()]
            if "sqlstride.yaml" not in lines:
                # ensure the previous line ends with a newline
                if lines and lines[-1] != "":
                    gi.write("\n")
                gi.write("sqlstride.yaml\n")
                click.echo("Added sqlstride.yaml to existing .gitignore")
    else:
        # Create .gitignore containing the entry
        with open(gitignore_path, "w", encoding="utf-8") as gi:
            gi.write("sqlstride.yaml\n")
        click.echo("Created .gitignore and added sqlstride.yaml")
    click.echo(f"Repository structure created at {project_path}")
    return