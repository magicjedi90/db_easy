# db_easy/adapters/__init__.py
from .postgres import PostgresAdapter
from .mssql    import MssqlAdapter
from .mariadb  import MariadbAdapter
from ..config import Config


def get_adapter(config: Config):
    name = config.sql_dialect  # e.g. "postgres"
    if name == "postgres":
        return PostgresAdapter(config)
    if name == "mssql":
        return MssqlAdapter(config)
    if name == "mariadb":
        return MariadbAdapter(config)
    raise ValueError(f"Unsupported dialect {name}")
