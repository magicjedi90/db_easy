# sqlstride/adapters/base.py
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Tuple, Iterable

import sqlparse
from etl.database.sql_dialects import SqlDialect
from etl.logger import Logger
from sqlalchemy import PoolProxiedConnection

from sqlstride.database.database_object import DatabaseObject

logger = Logger().get_logger()


class BaseAdapter(ABC):
    dialect: SqlDialect = None  # override in subclasses

    def __init__(self, connection: PoolProxiedConnection, default_schema: str, log_table: str, lock_table: str):
        self.connection = connection
        self.log_table = log_table
        self.lock_table = lock_table
        self.cursor = None
        self.default_schema = default_schema
        self.initialize_cursor()
        self.ensure_log_table()
        self.ensure_lock_table()

    def initialize_cursor(self):
        """Initialize the cursor if it's None."""
        if self.cursor is None and self.connection is not None:
            self.cursor = self.connection.cursor()

    def ensure_log_table(self):
        if self.dialect is None:
            raise ValueError("Cannot create log table: dialect is None")
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.default_schema}.{self.log_table} (
            {self.dialect.identity_fragment_function(self.log_table)},
            author varchar(100) NOT NULL,
            step_id varchar(100) NOT NULL,
            filename varchar(100) NOT NULL,
            checksum varchar(2000) NOT NULL,
            applied_at {self.dialect.datetime_type} DEFAULT NOW()
        );
        """
        self.execute(ddl)

    def ensure_lock_table(self):
        if self.dialect is None:
            raise ValueError("Cannot create lock table: dialect is None")
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.default_schema}.{self.lock_table} (
        {self.dialect.identity_fragment_function(self.lock_table)},
        locked_at {self.dialect.datetime_type} DEFAULT NOW()
        );
        """
        self.execute(ddl)

    # identical convenience wrappers the old psycopg code used:
    def execute(self, sql: str):
        self.initialize_cursor()
        if self.cursor is None:
            raise ValueError("Cannot execute SQL: cursor is None")
        self.cursor.execute(sql)

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def applied_steps(self) -> Dict[Tuple[str, str, str], str]:
        self.initialize_cursor()
        if self.cursor is None:
            return {}
        self.cursor.execute(
            f"SELECT author, step_id, filename, checksum FROM {self.default_schema}.{self.log_table};"
        )
        rows = self.cursor.fetchall()
        logger.debug(f"Found {len(rows)} applied steps")
        return {(row[0], row[1], row[2]): row[3] for row in rows}

    def record_step(self, step, checksum: str) -> None:
        values_placeholder = ", ".join([self.dialect.placeholder] * 4)
        values_placeholder = f"({values_placeholder})"
        self.cursor.execute(
            f"INSERT INTO {self.default_schema}.{self.log_table} "
            f"(author, step_id, filename, checksum) VALUES {values_placeholder};",
            (step.author, step.step_id, step.filename, checksum),
        )

    def lock(self):
        if self.dialect.name == "mssql":
            self.execute(f"INSERT INTO {self.default_schema}.{self.lock_table} DEFAULT VALUES;")
        else:
            self.execute(f"INSERT INTO {self.default_schema}.{self.lock_table} VALUES (DEFAULT, DEFAULT);")

    def unlock(self):
        self.execute(f"truncate table {self.default_schema}.{self.lock_table};")

    def is_locked(self):
        self.initialize_cursor()
        if self.cursor is None:
            return False
        self.cursor.execute(f"SELECT COUNT(*) FROM {self.default_schema}.{self.lock_table};")
        result = self.cursor.fetchone()
        if result is None:
            return False
        count = result[0]
        return count > 0

    @abstractmethod
    def discover_objects(self) -> Iterable[DatabaseObject]:
        """
        Yield DatabaseObject instances for every object in the live database.
        Subclasses implement all dialect quirks here.
        """
        ...

    def write_baseline(self, project_root: Path) -> int:
        """Iterate over objects and write one *.sql file each."""
        objects_written = 0
        for db_object in self.discover_objects():
            path = db_object.default_path(project_root)
            path.parent.mkdir(parents=True, exist_ok=True)
            if path.exists():
                continue  # skip pre-existing files
            file_text = f"-- step system:baseline\n\n{db_object.ddl.strip()}\n"
            file_text = sqlparse.format(file_text, reindent_aligned=True, keyword_case='upper', compact=True)
            path.write_text(file_text, encoding="utf-8")
            objects_written += 1
        return objects_written
