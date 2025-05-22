# db_easy/adapters/base.py
from abc import ABC, abstractmethod

from sqlalchemy import PoolProxiedConnection


class BaseAdapter(ABC):
    dialect = None  # override in subclasses

    def __init__(self, connection: PoolProxiedConnection, log_table: str):
        self.connection = connection
        self.log_table = log_table
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

    # identical convenience wrappers the old psycopg code used:
    def execute(self, sql: str):
        self.cursor.execute(sql)

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()
