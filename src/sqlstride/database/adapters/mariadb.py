# sqlstride/adapters/postgres.py
import re
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
            # Add IF NOT EXISTS if not already present
            if "CREATE TABLE" in ddl and "IF NOT EXISTS" not in ddl:
                ddl = ddl.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1)
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
            result = cur.fetchone()
            # SHOW CREATE VIEW returns: View, Create View, character_set_client, collation_connection
            # We need the second column (index 1) which contains the CREATE VIEW statement
            original_ddl = result[1] if result and len(result) > 1 else f"CREATE VIEW `{schema}`.`{view}` AS SELECT 1 AS placeholder;"

            # Clean up the view definition by extracting just the SELECT part
            # The pattern looks for anything after "VIEW `schema`.`name` AS " in the DDL
            select_part_match = re.search(r"VIEW\s+`[^`]+`\.`[^`]+`\s+AS\s+(.+)$", original_ddl, re.DOTALL)
            if select_part_match:
                select_part = select_part_match.group(1)
                # Create a clean CREATE OR REPLACE VIEW statement
                ddl = f"CREATE OR REPLACE VIEW `{schema}`.`{view}` AS {select_part}"
            else:
                # Fallback if regex doesn't match
                ddl = f"CREATE OR REPLACE VIEW `{schema}`.`{view}` AS SELECT 1 AS placeholder;"
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
            full_ddl = f"CREATE PROCEDURE IF NOT EXISTS `{schema}`.`{procedure}`({param_str})\n{ddl}"
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
            full_ddl = f"CREATE FUNCTION IF NOT EXISTS `{schema}`.`{function}`({param_str}) RETURNS {return_type}\n{ddl}"
            yield DatabaseObject("function", schema, function, full_ddl)

        # Triggers
        cur.execute(f"""
                    SELECT trigger_schema, trigger_name, action_statement, event_manipulation, event_object_table
                    FROM information_schema.triggers
                    WHERE trigger_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys');
                    """)
        for schema, trigger, action, event, table in cur.fetchall():
            ddl = f"CREATE TRIGGER IF NOT EXISTS `{schema}`.`{trigger}` {event} ON `{table}` FOR EACH ROW\n{action}"
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
                # Add IF NOT EXISTS if not already present
                if "CREATE SEQUENCE" in ddl and "IF NOT EXISTS" not in ddl:
                    ddl = ddl.replace("CREATE SEQUENCE", "CREATE SEQUENCE IF NOT EXISTS", 1)
                elif "CREATE TABLE" in ddl and "IF NOT EXISTS" not in ddl:
                    ddl = ddl.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1)
                yield DatabaseObject("sequence", schema, sequence, ddl)
        except:
            # Sequences might not be supported in this version
            pass
