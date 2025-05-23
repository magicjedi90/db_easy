# db_easy/adapters/postgres.py
from sqlalchemy import PoolProxiedConnection

from .base import BaseAdapter
from ..connector_proxy import build_connector
from ..dialects import postgres as postgres_dialect


class PostgresAdapter(BaseAdapter):

    dialect = postgres_dialect

    def __init__(self, config):
        connection: PoolProxiedConnection = build_connector(config).to_user_postgres()
        super().__init__(connection, config.default_schema, config.log_table, config.lock_table)
