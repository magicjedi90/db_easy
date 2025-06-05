# sqlstride/adapters/postgres.py
from etl.database.sql_dialects import mariadb
from sqlalchemy import PoolProxiedConnection

from .base import BaseAdapter
from sqlstride.database.connector_proxy import build_connector
from ..database_object import DatabaseObject


class MariadbAdapter(BaseAdapter):

    dialect = mariadb

    def __init__(self, config):
        connection: PoolProxiedConnection = build_connector(config).to_user_mysql()
        super().__init__(connection, config.default_schema, config.log_table, config.lock_table)

    def discover_objects(self):
        cur = self.cursor

        # Tables
        cur.execute(f"""
                    SELECT table_schema, table_name
                    FROM information_schema.tables
                    WHERE table_type = 'BASE TABLE'
                      AND table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys');
                    """)
        for schema, table in cur.fetchall():
            # Get table DDL
            cur.execute(f"SHOW CREATE TABLE `{schema}`.`{table}`;")
            _, ddl = cur.fetchone()
            yield DatabaseObject("table", schema, table, ddl)

        # Views
        cur.execute(f"""
                    SELECT table_schema, table_name
                    FROM information_schema.views
                    WHERE table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys');
                    """)
        for schema, view in cur.fetchall():
            # Get view DDL
            cur.execute(f"SHOW CREATE VIEW `{schema}`.`{view}`;")
            _, _, ddl, _ = cur.fetchone()
            yield DatabaseObject("view", schema, view, ddl)

        # Procedures
        cur.execute(f"""
                    SELECT routine_schema, routine_name, routine_definition
                    FROM information_schema.routines
                    WHERE routine_type = 'PROCEDURE'
                      AND routine_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys');
                    """)
        for schema, procedure, ddl in cur.fetchall():
            # Get procedure parameters
            cur.execute(f"""
                        SELECT parameter_mode, parameter_name, data_type
                        FROM information_schema.parameters
                        WHERE specific_schema = '{schema}'
                          AND specific_name = '{procedure}'
                        ORDER BY ordinal_position;
                        """)
            params = []
            for mode, name, data_type in cur.fetchall():
                if name:
                    params.append(f"{mode} {name} {data_type}")
                else:
                    params.append(f"{data_type}")

            param_str = ", ".join(params)
            full_ddl = f"CREATE PROCEDURE `{schema}`.`{procedure}`({param_str})\n{ddl}"
            yield DatabaseObject("procedure", schema, procedure, full_ddl)

        # Functions
        cur.execute(f"""
                    SELECT routine_schema, routine_name, routine_definition, data_type
                    FROM information_schema.routines
                    WHERE routine_type = 'FUNCTION'
                      AND routine_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys');
                    """)
        for schema, function, ddl, return_type in cur.fetchall():
            # Get function parameters
            cur.execute(f"""
                        SELECT parameter_mode, parameter_name, data_type
                        FROM information_schema.parameters
                        WHERE specific_schema = '{schema}'
                          AND specific_name = '{function}'
                          AND parameter_name IS NOT NULL
                        ORDER BY ordinal_position;
                        """)
            params = []
            for mode, name, data_type in cur.fetchall():
                params.append(f"{name} {data_type}")

            param_str = ", ".join(params)
            full_ddl = f"CREATE FUNCTION `{schema}`.`{function}`({param_str}) RETURNS {return_type}\n{ddl}"
            yield DatabaseObject("function", schema, function, full_ddl)

        # Triggers
        cur.execute(f"""
                    SELECT trigger_schema, trigger_name, action_statement, event_manipulation, event_object_table
                    FROM information_schema.triggers
                    WHERE trigger_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys');
                    """)
        for schema, trigger, action, event, table in cur.fetchall():
            ddl = f"CREATE TRIGGER `{schema}`.`{trigger}` {event} ON `{table}` FOR EACH ROW\n{action}"
            yield DatabaseObject("trigger", schema, trigger, ddl)

        # Sequences (MariaDB 10.3+)
        try:
            cur.execute(f"""
                        SELECT table_schema, table_name
                        FROM information_schema.tables
                        WHERE table_type = 'SEQUENCE'
                          AND table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys');
                        """)
            for schema, sequence in cur.fetchall():
                # Get sequence DDL
                cur.execute(f"SHOW CREATE TABLE `{schema}`.`{sequence}`;")
                _, ddl = cur.fetchone()
                yield DatabaseObject("sequence", schema, sequence, ddl)
        except:
            # Sequences might not be supported in this version
            pass
