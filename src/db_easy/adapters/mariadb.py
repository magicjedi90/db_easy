# db_easy/adapters/postgres.py
from etl.database.sql_dialects import mariadb
from sqlalchemy import PoolProxiedConnection

from .base import BaseAdapter
from ..connector_proxy import build_connector


class MariadbAdapter(BaseAdapter):

    dialect = mariadb

    def __init__(self, config):
        connection: PoolProxiedConnection = build_connector(config).to_user_mysql()
        super().__init__(connection, config.default_schema, config.log_table, config.lock_table)
