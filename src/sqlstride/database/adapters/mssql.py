# sqlstride/adapters/mssql.py
from etl.database.sql_dialects import mssql
from sqlalchemy import PoolProxiedConnection

from .base import BaseAdapter
from sqlstride.config import Config
from sqlstride.database.connector_proxy import build_connector


class MssqlAdapter(BaseAdapter):

    dialect = mssql

    def __init__(self, config: Config):
        if config.trusted_auth:
            connection: PoolProxiedConnection = build_connector(config).to_trusted_msql()
        else:
            connection: PoolProxiedConnection = build_connector(config).to_user_msql()

        super().__init__(connection, config.default_schema, config.log_table, config.lock_table)

    def ensure_log_table(self):
        ddl = f"""
        IF NOT EXISTS (
            SELECT 1
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{self.default_schema}'
            AND TABLE_NAME = '{self.log_table}'
        )
        BEGIN
            CREATE TABLE {self.default_schema}].[{self.log_table}
            (
                id           INT IDENTITY(1,1) PRIMARY KEY,   -- identity column
                author       VARCHAR(100)  NOT NULL,
                step_id      VARCHAR(100)  NOT NULL,
                filename     VARCHAR(100)  NOT NULL,
                checksum     VARCHAR(2000) NOT NULL,
                applied_at   DATETIME2      DEFAULT (SYSDATETIME())
            );
        END;
        """

        self.execute(ddl)

    def ensure_lock_table(self):

        ddl = f"""
        IF NOT EXISTS (
            SELECT 1
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{self.default_schema}'
            AND TABLE_NAME = '{self.lock_table}'
        )
        BEGIN
            CREATE TABLE {self.default_schema}].[{self.lock_table}
            (
                {self.dialect.identity_fragment_function(self.lock_table)},
                locked_at {self.dialect.datetime_type} DEFAULT NOW()
            );
        END;
        """

        self.execute(ddl)

    def lock(self):
        self.execute(f"INSERT INTO {self.default_schema}.{self.lock_table} DEFAULT VALUES;")

    def discover_objects(self):
        cur = self.cursor

        # Tables
        cur.execute(f"""
                    SELECT 
                        s.name AS schema_name,
                        t.name AS table_name
                    FROM sys.tables t
                    JOIN sys.schemas s ON t.schema_id = s.schema_id
                    WHERE s.name NOT IN ('sys', 'INFORMATION_SCHEMA');
                    """)
        for schema, table in cur.fetchall():
            # Get table DDL
            cur.execute(f"""
                        SELECT 
                            'CREATE TABLE [' + s.name + '].[' + t.name + '] (' +
                            STRING_AGG(
                                CAST(
                                    '[' + c.name + '] ' + 
                                    CASE 
                                        WHEN t.name = 'sysname' THEN 'sysname'
                                        ELSE 
                                            tp.name + 
                                            CASE 
                                                WHEN tp.name IN ('varchar', 'nvarchar', 'char', 'nchar') 
                                                    THEN '(' + CASE WHEN c.max_length = -1 THEN 'MAX' ELSE CAST(c.max_length AS VARCHAR) END + ')'
                                                WHEN tp.name IN ('decimal', 'numeric') 
                                                    THEN '(' + CAST(c.precision AS VARCHAR) + ',' + CAST(c.scale AS VARCHAR) + ')'
                                                ELSE ''
                                            END
                                    END +
                                    CASE WHEN c.is_nullable = 0 THEN ' NOT NULL' ELSE ' NULL' END +
                                    CASE WHEN ic.is_identity = 1 THEN ' IDENTITY(' + CAST(IDENT_SEED(s.name + '.' + t.name) AS VARCHAR) + ',' + CAST(IDENT_INCR(s.name + '.' + t.name) AS VARCHAR) + ')' ELSE '' END +
                                    CASE WHEN dc.definition IS NOT NULL THEN ' DEFAULT ' + dc.definition ELSE '' END
                                AS NVARCHAR(MAX)), 
                                ',' + CHAR(13) + CHAR(10) + '    '
                            ) +
                            CASE 
                                WHEN pk.name IS NOT NULL THEN 
                                    ',' + CHAR(13) + CHAR(10) + '    CONSTRAINT [' + pk.name + '] PRIMARY KEY (' +
                                    (SELECT STRING_AGG(CAST('[' + c.name + ']' AS NVARCHAR(MAX)), ',') 
                                     FROM sys.index_columns ic
                                     JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                                     WHERE ic.object_id = t.object_id AND ic.index_id = pk.index_id)
                                    + ')'
                                ELSE ''
                            END +
                            ')' AS table_ddl
                        FROM sys.tables t
                        JOIN sys.schemas s ON t.schema_id = s.schema_id
                        JOIN sys.columns c ON t.object_id = c.object_id
                        JOIN sys.types tp ON c.user_type_id = tp.user_type_id
                        LEFT JOIN sys.identity_columns ic ON c.object_id = ic.object_id AND c.column_id = ic.column_id
                        LEFT JOIN sys.default_constraints dc ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                        LEFT JOIN sys.indexes pk ON t.object_id = pk.object_id AND pk.is_primary_key = 1
                        WHERE s.name = '{schema}' AND t.name = '{table}'
                        GROUP BY s.name, t.name, pk.name, pk.index_id;
                        """)
            ddl_row = cur.fetchone()
            if ddl_row:
                create_table_ddl = ddl_row[0]
                # Wrap the CREATE TABLE statement in an IF NOT EXISTS check
                ddl = f"""
IF NOT EXISTS (
    SELECT 1
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = '{schema}'
    AND TABLE_NAME = '{table}'
)
BEGIN
    {create_table_ddl}
END;
"""
                yield DatabaseObject("table", schema, table, ddl)

        # Views
        cur.execute(f"""
                    SELECT 
                        s.name AS schema_name,
                        v.name AS view_name,
                        m.definition AS view_definition
                    FROM sys.views v
                    JOIN sys.schemas s ON v.schema_id = s.schema_id
                    JOIN sys.sql_modules m ON v.object_id = m.object_id
                    WHERE s.name NOT IN ('sys', 'INFORMATION_SCHEMA');
                    """)
        for schema, view, definition in cur.fetchall():
            # Use CREATE OR ALTER VIEW for idempotent creation
            ddl = f"CREATE OR ALTER VIEW [{schema}].[{view}] AS\n{definition}"
            yield DatabaseObject("view", schema, view, ddl)

        # Stored Procedures
        cur.execute(f"""
                    SELECT 
                        s.name AS schema_name,
                        p.name AS procedure_name,
                        m.definition AS procedure_definition
                    FROM sys.procedures p
                    JOIN sys.schemas s ON p.schema_id = s.schema_id
                    JOIN sys.sql_modules m ON p.object_id = m.object_id
                    WHERE s.name NOT IN ('sys', 'INFORMATION_SCHEMA');
                    """)
        for schema, procedure, definition in cur.fetchall():
            # Ensure the procedure definition uses CREATE OR ALTER PROCEDURE
            if definition.strip().upper().startswith("CREATE PROCEDURE"):
                definition = definition.replace("CREATE PROCEDURE", "CREATE OR ALTER PROCEDURE", 1)
            yield DatabaseObject("procedure", schema, procedure, definition)

        # Functions
        cur.execute(f"""
                    SELECT 
                        s.name AS schema_name,
                        o.name AS function_name,
                        m.definition AS function_definition,
                        CASE 
                            WHEN o.type = 'FN' THEN 'SCALAR'
                            WHEN o.type = 'TF' THEN 'TABLE'
                            WHEN o.type = 'IF' THEN 'INLINE_TABLE'
                            ELSE o.type
                        END AS function_type
                    FROM sys.objects o
                    JOIN sys.schemas s ON o.schema_id = s.schema_id
                    JOIN sys.sql_modules m ON o.object_id = m.object_id
                    WHERE o.type IN ('FN', 'TF', 'IF')
                    AND s.name NOT IN ('sys', 'INFORMATION_SCHEMA');
                    """)
        for schema, function, definition, function_type in cur.fetchall():
            # Ensure the function definition uses CREATE OR ALTER FUNCTION
            if definition.strip().upper().startswith("CREATE FUNCTION"):
                definition = definition.replace("CREATE FUNCTION", "CREATE OR ALTER FUNCTION", 1)
            yield DatabaseObject("function", schema, function, definition)

        # Triggers
        cur.execute(f"""
                    SELECT 
                        s.name AS schema_name,
                        t.name AS trigger_name,
                        m.definition AS trigger_definition,
                        OBJECT_NAME(t.parent_id) AS table_name
                    FROM sys.triggers t
                    JOIN sys.schemas s ON t.schema_id = s.schema_id
                    JOIN sys.sql_modules m ON t.object_id = m.object_id
                    WHERE s.name NOT IN ('sys', 'INFORMATION_SCHEMA');
                    """)
        for schema, trigger, definition, table_name in cur.fetchall():
            # Ensure the trigger definition uses CREATE OR ALTER TRIGGER
            if definition.strip().upper().startswith("CREATE TRIGGER"):
                definition = definition.replace("CREATE TRIGGER", "CREATE OR ALTER TRIGGER", 1)
            yield DatabaseObject("trigger", schema, trigger, definition)

        # Sequences
        cur.execute(f"""
                    SELECT 
                        s.name AS schema_name,
                        seq.name AS sequence_name,
                        'CREATE SEQUENCE [' + s.name + '].[' + seq.name + ']' +
                        ' AS ' + t.name +
                        ' START WITH ' + CAST(seq.start_value AS VARCHAR) +
                        ' INCREMENT BY ' + CAST(seq.increment AS VARCHAR) +
                        CASE 
                            WHEN seq.minimum_value IS NOT NULL THEN ' MINVALUE ' + CAST(seq.minimum_value AS VARCHAR)
                            ELSE ' NO MINVALUE'
                        END +
                        CASE 
                            WHEN seq.maximum_value IS NOT NULL THEN ' MAXVALUE ' + CAST(seq.maximum_value AS VARCHAR)
                            ELSE ' NO MAXVALUE'
                        END +
                        CASE 
                            WHEN seq.is_cycling = 1 THEN ' CYCLE'
                            ELSE ' NO CYCLE'
                        END +
                        CASE 
                            WHEN seq.is_cached = 1 THEN ' CACHE ' + CAST(seq.cache_size AS VARCHAR)
                            ELSE ' NO CACHE'
                        END AS sequence_ddl
                    FROM sys.sequences seq
                    JOIN sys.schemas s ON seq.schema_id = s.schema_id
                    JOIN sys.types t ON seq.user_type_id = t.user_type_id
                    WHERE s.name NOT IN ('sys', 'INFORMATION_SCHEMA');
                    """)
        for schema, sequence, sequence_ddl in cur.fetchall():
            # Wrap the CREATE SEQUENCE statement in an IF NOT EXISTS check
            ddl = f"""
IF NOT EXISTS (
    SELECT 1
    FROM sys.sequences s
    JOIN sys.schemas sch ON s.schema_id = sch.schema_id
    WHERE sch.name = '{schema}'
    AND s.name = '{sequence}'
)
BEGIN
    {sequence_ddl}
END;
"""
            yield DatabaseObject("sequence", schema, sequence, ddl)

        # Types (User-Defined Types)
        cur.execute(f"""
                    SELECT 
                        s.name AS schema_name,
                        t.name AS type_name,
                        CASE 
                            WHEN t.is_table_type = 1 THEN 'TABLE'
                            ELSE 'SCALAR'
                        END AS type_kind,
                        bt.name AS base_type,
                        t.max_length,
                        t.precision,
                        t.scale
                    FROM sys.types t
                    JOIN sys.schemas s ON t.schema_id = s.schema_id
                    JOIN sys.types bt ON t.system_type_id = bt.user_type_id
                    WHERE t.is_user_defined = 1
                    AND s.name NOT IN ('sys', 'INFORMATION_SCHEMA');
                    """)
        for schema, type_name, type_kind, base_type, max_length, precision, scale in cur.fetchall():
            if type_kind == 'TABLE':
                # Get table type definition
                cur.execute(f"""
                            SELECT 
                                'CREATE TYPE [{schema}].[{type_name}] AS TABLE (' +
                                STRING_AGG(
                                    CAST(
                                        '[' + c.name + '] ' + 
                                        CASE 
                                            WHEN t.name = 'sysname' THEN 'sysname'
                                            ELSE 
                                                t.name + 
                                                CASE 
                                                    WHEN t.name IN ('varchar', 'nvarchar', 'char', 'nchar') 
                                                        THEN '(' + CASE WHEN c.max_length = -1 THEN 'MAX' ELSE CAST(c.max_length AS VARCHAR) END + ')'
                                                    WHEN t.name IN ('decimal', 'numeric') 
                                                        THEN '(' + CAST(c.precision AS VARCHAR) + ',' + CAST(c.scale AS VARCHAR) + ')'
                                                    ELSE ''
                                                END
                                        END +
                                        CASE WHEN c.is_nullable = 0 THEN ' NOT NULL' ELSE ' NULL' END
                                    AS NVARCHAR(MAX)), 
                                    ',' + CHAR(13) + CHAR(10) + '    '
                                ) +
                                ')' AS type_ddl
                            FROM sys.table_types tt
                            JOIN sys.columns c ON tt.type_table_object_id = c.object_id
                            JOIN sys.types t ON c.user_type_id = t.user_type_id
                            WHERE tt.schema_id = SCHEMA_ID('{schema}')
                            AND tt.name = '{type_name}'
                            GROUP BY tt.name;
                            """)
                ddl_row = cur.fetchone()
                if ddl_row:
                    type_ddl = ddl_row[0]
                    # Wrap the CREATE TYPE statement in an IF NOT EXISTS check
                    ddl = f"""
IF NOT EXISTS (
    SELECT 1
    FROM sys.types t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = '{schema}'
    AND t.name = '{type_name}'
)
BEGIN
    {type_ddl}
END;
"""
                    yield DatabaseObject("type", schema, type_name, ddl)
            else:
                # Scalar type
                type_def = base_type
                if base_type in ('varchar', 'nvarchar', 'char', 'nchar'):
                    if max_length == -1:
                        type_def += '(MAX)'
                    else:
                        type_def += f'({max_length})'
                elif base_type in ('decimal', 'numeric'):
                    type_def += f'({precision},{scale})'

                scalar_type_ddl = f"CREATE TYPE [{schema}].[{type_name}] FROM {type_def}"
                # Wrap the CREATE TYPE statement in an IF NOT EXISTS check
                ddl = f"""
IF NOT EXISTS (
    SELECT 1
    FROM sys.types t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = '{schema}'
    AND t.name = '{type_name}'
)
BEGIN
    {scalar_type_ddl}
END;
"""
                yield DatabaseObject("type", schema, type_name, ddl)
