# db_easy/adapters/base.py
from abc import ABC, abstractmethod
from typing import Dict, Tuple

from etl.database.sql_dialects import SqlDialect
from sqlalchemy import PoolProxiedConnection


class BaseAdapter(ABC):
    dialect: SqlDialect = None  # override in subclasses

    def __init__(self, connection: PoolProxiedConnection, log_table: str, lock_table: str):
        self.connection = connection
        self.log_table = log_table
        self.lock_table = lock_table
        self.ensure_log_table()
        self.cursor = self.connection.cursor()

    def ensure_log_table(self):
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.log_table} (
            {self.dialect.identity_fragment_function(self.log_table)},
            author varchar(100) NOT NULL,
            step_id varchar(100) NOT NULL,
            filename varchar(100) NOT NULL,
            checksum varchar(256) NOT NULL,
            applied_at {self.dialect.datetime_type} DEFAULT NOW()
        );
        """
        self.cursor.execute(ddl)

    def ensure_lock_table(self):
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.lock_table} (
        {self.dialect.identity_fragment_function(self.lock_table)},
        locked_at {self.dialect.datetime_type} DEFAULT NOW()
        );
        """
        self.cursor.execute(ddl)
    # identical convenience wrappers the old psycopg code used:
    def execute(self, sql: str):
        self.cursor.execute(sql)

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def applied_steps(self) -> Dict[Tuple[str, str, str], str]:
        rows = self.cursor.execute(
            f"SELECT author, step_id, filename, checksum FROM {self.log_table};"
        ).fetchall()
        return {(row[0], row[1], row[2]): row[3] for row in rows}

    def record_step(self, step, checksum: str) -> None:
        values_placeholder = ", ".join([self.dialect.placeholder] * 4)
        values_placeholder = f"({values_placeholder})"
        self.cursor.execute(
            f"INSERT INTO {self.log_table} "
            f"(author, step_id, filename, checksum) VALUES {values_placeholder};",
            (step.author, step.step_id, step.filename, checksum),
        )

    def lock(self):
        self.cursor.execute(f"INSERT INTO {self.lock_table} VALUES (DEFAULT);")

    def unlock(self):
        self.cursor.execute(f"truncate table {self.lock_table};")

    def is_locked(self):
        return self.cursor.execute(f"SELECT COUNT(*) FROM {self.lock_table};").fetchone()[0] > 0
