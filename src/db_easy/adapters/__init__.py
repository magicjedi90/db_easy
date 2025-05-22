# db_easy/adapters/__init__.py
from .postgres import PostgresAdapter
from .mssql    import MssqlAdapter
from .mariadb  import MariadbAdapter

def get_adapter(cfg):
    name = cfg.dialect_name  # e.g. "postgres"
    if name == "postgres":
        return PostgresAdapter(cfg)
    if name == "mssql":
        return MssqlAdapter(cfg)
    if name == "mariadb":
        return MariadbAdapter(cfg)
    raise ValueError(f"Unsupported dialect {name}")
