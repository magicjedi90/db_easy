# db_easy/dialects.py
from etl.database.sql_dialects import mssql, mariadb, postgres

REGISTRY = {dialect.name: dialect for dialect in (mssql, mariadb, postgres)}
