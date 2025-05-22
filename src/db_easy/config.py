# db_easy/config.py
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Any

import yaml


@dataclass
class Config:
    project_path: Path
    host: str
    port: int
    instance: str
    database: str
    username: str
    password: str
    trusted: str

    schema_path: Path
    log_table: str = "db_easy_log"
    lock_table: str = "db_easy_lock"
    template_vars: Dict[str, Any] = field(default_factory=dict)


def load_config(
    project_path: Path,
    db_url_override: str | None = None,
    cli_vars: str | None = None,
) -> Config:
    """Read db-easy.yaml, merge env vars & CLI overrides, return a Config object."""
    config_file = project_path / "db-easy.yaml"
    if not config_file.exists():
        raise FileNotFoundError("db-easy.yaml not found in the project path")

    data = yaml.safe_load(config_file.read_text()) or {}

    # Determine DB URL
    db_url = (
        db_url_override
        or data.get("database_url")
        or _build_url_from_parts(data.get("database", {}))
    )
    if not db_url:
        raise ValueError("Database URL missing—supply it in db-easy.yaml or --db")

    schema_path = project_path / data.get("schema_path", "schema")
    log_table = data.get("log_table", "migration_log")
    template_vars = data.get("template_vars", {})

    # Merge in CLI-supplied template vars
    if cli_vars:
        for kv in cli_vars.split(","):
            if "=" in kv:
                k, v = kv.split("=", 1)
                template_vars[k.strip()] = v.strip()

    return Config(project_path, db_url, schema_path, log_table, template_vars)


def _build_url_from_parts(parts: dict) -> str | None:
    """Allow users to specify connection parts instead of a full URL."""
    if not parts:
        return None
    driver = parts.get("driver", "postgresql")
    user = parts.get("user")
    password = (
        os.getenv(parts.get("password_env"))
        if "password_env" in parts
        else parts.get("password")
    )
    host = parts.get("host", "localhost")
    port = parts.get("port", 5432)
    database = parts.get("database")
    if not (user and password and database):
        return None
    return f"{driver}://{user}:{password}@{host}:{port}/{database}"
