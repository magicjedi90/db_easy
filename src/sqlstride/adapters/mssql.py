# sqlstride/adapters/mssql.py
from etl.database.sql_dialects import mssql
from sqlalchemy import PoolProxiedConnection

from .base import BaseAdapter
from ..config import Config
from ..connector_proxy import build_connector


class MssqlAdapter(BaseAdapter):

    dialect = mssql

    def __init__(self, config: Config):
        if config.trusted_auth:
            connection: PoolProxiedConnection = build_connector(config).to_trusted_msql()
        else:
            connection: PoolProxiedConnection = build_connector(config).to_user_msql()

        super().__init__(connection, config.default_schema, config.log_table, config.lock_table)

    def ensure_log_table(self):
        ddl = f"""
        IF NOT EXISTS (
            SELECT 1
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{self.default_schema}'
            AND TABLE_NAME = '{self.log_table}'
        )
        BEGIN
            CREATE TABLE {self.default_schema}].[{self.log_table}
            (
                id           INT IDENTITY(1,1) PRIMARY KEY,   -- identity column
                author       VARCHAR(100)  NOT NULL,
                step_id      VARCHAR(100)  NOT NULL,
                filename     VARCHAR(100)  NOT NULL,
                checksum     VARCHAR(2000) NOT NULL,
                applied_at   DATETIME2      DEFAULT (SYSDATETIME())
            );
        END;
        """

        self.execute(ddl)

    def ensure_lock_table(self):

        ddl = f"""
        IF NOT EXISTS (
            SELECT 1
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{self.default_schema}'
            AND TABLE_NAME = '{self.lock_table}'
        )
        BEGIN
            CREATE TABLE {self.default_schema}].[{self.lock_table}
            (
                {self.dialect.identity_fragment_function(self.lock_table)},
                locked_at {self.dialect.datetime_type} DEFAULT NOW()
            );
        END;
        """

        self.execute(ddl)

    def lock(self):
        self.execute(f"INSERT INTO {self.default_schema}.{self.lock_table} DEFAULT VALUES;")
