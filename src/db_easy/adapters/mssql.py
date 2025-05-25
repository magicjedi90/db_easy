# db_easy/adapters/mssql.py
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