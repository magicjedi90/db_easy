# db_easy/connector_proxy.py
from etl.database.connector import Connector

from .config import Config


def build_connector(config: Config) -> Connector:
    """Instantiate Connector from db-easy config section."""
    # split cfg.db_url or use discrete fields; illustration only:
    return Connector(
        host     = config.host,
        port     = config.port,
        instance = config.instance,   # MSSQL only
        database = config.database,
        username = config.username,
        password = config.password,
    )
