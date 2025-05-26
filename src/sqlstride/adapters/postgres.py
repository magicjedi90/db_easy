# sqlstride/adapters/postgres.py
from sqlalchemy import PoolProxiedConnection

from .base import BaseAdapter
from ..connector_proxy import build_connector
from etl.database.sql_dialects import postgres


class PostgresAdapter(BaseAdapter):

    dialect = postgres

    def __init__(self, config):
        connection: PoolProxiedConnection = build_connector(config).to_user_postgres()
        super().__init__(connection, config.default_schema, config.log_table, config.lock_table)
