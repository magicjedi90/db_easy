# db_easy/executor.py
import os
from hashlib import sha256
from pathlib import Path

import click

from .adapters import get_adapter
from .constants import ORDERED_DIRS
from .parser import parse_directory
from .templating import render_sql


def sync_database(config, *, dry_run: bool = False, same_checksums: bool = False) -> None:
    adapter = get_adapter(config)
    if adapter.is_locked():
        raise Exception("db-easy is already running")
    all_steps = parse_directory(Path(config.schema_path))
    applied = adapter.get_applied_steps()
    if same_checksums:
        different_checksums = []
        #  if applied checksums are different from new checksums raise error
        already_applied = [step for step in all_steps if (step.filename, step.author, step.step_id) in applied]
        for step in already_applied:
            sql_rendered = render_sql(step.sql, config.template_vars, step.filename)
            checksum = sha256(sql_rendered.encode()).hexdigest()
            if checksum != applied[step.filename, step.author, step.step_id]:
                different_checksums.append((step.filename, step.author, step.step_id))
        if different_checksums:
            different_checksum_string = "\n".join(
                f"{filename} {author}:{step_id}" for filename, author, step_id in different_checksums)
            raise Exception(f"Checksums for the following steps are different:\n{different_checksum_string}")

    pending = [step for step in all_steps if (step.filename, step.author, step.step_id) not in applied]

    if not pending:
        print("✔ Database is already up to date.")
        return

    for step in pending:
        sql_rendered = render_sql(step.sql, config.template_vars, step.filename)
        checksum = sha256(sql_rendered.encode()).hexdigest()

        if dry_run:
            print(f"\n-- WOULD APPLY {step.author}:{step.step_id} ({step.filename})")
            print(sql_rendered)
            continue

        try:
            adapter.lock()
            adapter.execute(sql_rendered)
            adapter.record_step(step, checksum)
            adapter.unlock()
            adapter.commit()
            print(f"✓ Applied {step.filename} {step.author}:{step.step_id}")
        except Exception as exc:
            adapter.rollback()
            raise RuntimeError(
                f"Failed on {step.filename} {step.author}:{step.step_id} → {exc}"
            ) from exc


def create_repository_structure(project_path: str) -> None:
    """
    Create the repository structure with all directories from ORDERED_DIRS
    and a blank __init__.py file in each directory. Also creates a db-easy.yaml
    file with user-provided configuration.

    Args:
        project_path: Path to the project directory
    """
    project_path = Path(project_path)

    # Get user input for the db-easy.yaml file
    click.echo("Creating db-easy.yaml configuration file...")

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
    log_table = click.prompt("Log table name", default='db_easy_log')
    lock_table = click.prompt("Lock table name", default='db_easy_lock')

    # Create the db-easy.yaml file
    yaml_file = project_path / "db-easy.yaml"
    with open(yaml_file, 'w') as file:
        file.write("# DB-Easy Configuration\n\n")
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

    click.echo(f"Created db-easy.yaml configuration file at {yaml_file}")

    # Create each directory and add __init__.py
    for directory in ORDERED_DIRS:
        dir_path = project_path / directory
        os.makedirs(dir_path, exist_ok=True)

        # Create blank __init__.py file
        init_file = dir_path / "__init__.py"
        with open(init_file, 'w') as f:
            pass  # Create an empty file

    click.echo(f"Repository structure created at {project_path}")
