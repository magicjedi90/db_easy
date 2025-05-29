import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path
from sqlstride.adapters import get_adapter
from sqlstride.adapters.base import BaseAdapter
from sqlstride.config import Config
from sqlstride.parser import Step
from etl.database.sql_dialects import postgres


class TestAdapter(BaseAdapter):
    """Test adapter with postgres dialect for testing."""
    dialect = postgres


@pytest.fixture
def mock_connection():
    """Create a mock database connection."""
    connection = MagicMock()
    cursor = MagicMock()
    connection.cursor.return_value = cursor
    return connection, cursor


@pytest.fixture
def mock_connector():
    """Create a mock connector that returns a mock connection."""
    connector = MagicMock()
    connection = MagicMock()
    cursor = MagicMock()
    connection.cursor.return_value = cursor

    # Set up the connector to return the connection
    connector.to_user_mysql.return_value = connection
    connector.to_user_postgres.return_value = connection
    connector.to_user_mssql.return_value = connection
    connector.to_trusted_msql.return_value = connection

    return connector, connection, cursor


class TestBaseAdapter:
    """Tests for the BaseAdapter class."""

    def test_initialize_cursor(self, mock_connection):
        """Test initializing the cursor."""
        connection, cursor = mock_connection
        adapter = TestAdapter(connection, "public", "sqlstride_log", "sqlstride_lock")

        # The cursor should be initialized during __init__
        assert adapter.cursor is not None
        connection.cursor.assert_called_once()

    def test_execute(self, mock_connection):
        """Test executing SQL."""
        connection, cursor = mock_connection
        adapter = TestAdapter(connection, "public", "sqlstride_log", "sqlstride_lock")

        adapter.execute("SELECT 1")
        cursor.execute.assert_called_with("SELECT 1")

    def test_commit(self, mock_connection):
        """Test committing a transaction."""
        connection, cursor = mock_connection
        adapter = TestAdapter(connection, "public", "sqlstride_log", "sqlstride_lock")

        adapter.commit()
        connection.commit.assert_called_once()

    def test_rollback(self, mock_connection):
        """Test rolling back a transaction."""
        connection, cursor = mock_connection
        adapter = TestAdapter(connection, "public", "sqlstride_log", "sqlstride_lock")

        adapter.rollback()
        connection.rollback.assert_called_once()

    def test_applied_steps(self, mock_connection):
        """Test getting applied steps."""
        connection, cursor = mock_connection
        adapter = TestAdapter(connection, "public", "sqlstride_log", "sqlstride_lock")

        # Mock the cursor to return some applied steps
        cursor.fetchall.return_value = [
            ("author1", "step1", "file1.sql", "checksum1"),
            ("author2", "step2", "file2.sql", "checksum2")
        ]

        applied = adapter.applied_steps()

        # Check that the query was executed
        cursor.execute.assert_called_with(
            "SELECT author, step_id, filename, checksum FROM public.sqlstride_log;"
        )

        # Check that the applied steps were returned correctly
        assert applied[("author1", "step1", "file1.sql")] == "checksum1"
        assert applied[("author2", "step2", "file2.sql")] == "checksum2"

    def test_record_step(self, mock_connection):
        """Test recording a step."""
        connection, cursor = mock_connection
        adapter = TestAdapter(connection, "public", "sqlstride_log", "sqlstride_lock")

        # Create a step
        step = Step(author="author1", step_id="step1", sql="SELECT 1", filename="file1.sql")

        # Record the step
        adapter.record_step(step, "checksum1")

        # Check that the insert was executed
        cursor.execute.assert_called()
        # The exact query depends on the dialect, so we just check that it was called

    def test_lock_unlock(self, mock_connection):
        """Test locking and unlocking."""
        connection, cursor = mock_connection
        adapter = TestAdapter(connection, "public", "sqlstride_log", "sqlstride_lock")

        # Lock
        adapter.lock()
        cursor.execute.assert_called()

        # Reset the mock
        cursor.execute.reset_mock()

        # Unlock
        adapter.unlock()
        cursor.execute.assert_called_with("truncate table public.sqlstride_lock;")

    def test_is_locked(self, mock_connection):
        """Test checking if the database is locked."""
        connection, cursor = mock_connection
        adapter = TestAdapter(connection, "public", "sqlstride_log", "sqlstride_lock")

        # Mock the cursor to return a count of 1 (locked)
        cursor.fetchone.return_value = (1,)

        assert adapter.is_locked() is True

        # Check that the query was executed
        cursor.execute.assert_called_with("SELECT COUNT(*) FROM public.sqlstride_lock;")

        # Reset the mock
        cursor.execute.reset_mock()
        cursor.fetchone.reset_mock()

        # Mock the cursor to return a count of 0 (not locked)
        cursor.fetchone.return_value = (0,)

        assert adapter.is_locked() is False

        # Check that the query was executed
        cursor.execute.assert_called_with("SELECT COUNT(*) FROM public.sqlstride_lock;")

    def test_null_dialect_raises(self, mock_connection):
        """Test ensure_log_table with null dialect."""
        connection, cursor = mock_connection

        # Create a subclass of BaseAdapter with dialect set to None
        class NullAdapter(BaseAdapter):
            dialect = None


        # Call ensure_log_table and check that it raises a ValueError
        with pytest.raises(ValueError, match="Cannot create log table: dialect is None"):
            NullAdapter(connection, "public", "sqlstride_log", "sqlstride_lock")


@patch("sqlstride.adapters.postgres.build_connector")
def test_get_adapter_postgres(mock_build_connector, mock_connector):
    """Test getting a Postgres adapter."""
    connector, connection, cursor = mock_connector
    mock_build_connector.return_value = connector

    config = Config(
        project_path=Path("/fake/path"),
        host="localhost",
        port=5432,
        instance="",
        database="test_db",
        username="test_user",
        password="test_password",
        trusted_auth=False,
        sql_dialect="postgres",
        default_schema="public",
        log_table="sqlstride_log",
        lock_table="sqlstride_lock",
        jinja_vars={}
    )

    adapter = get_adapter(config)

    # Check that the connector was built with the correct config
    mock_build_connector.assert_called_once_with(config)

    # Check that the correct connection method was called
    connector.to_user_postgres.assert_called_once()

    # Check that the adapter has the correct attributes
    assert adapter.default_schema == "public"
    assert adapter.log_table == "sqlstride_log"
    assert adapter.lock_table == "sqlstride_lock"


@patch("sqlstride.adapters.mariadb.build_connector")
def test_get_adapter_mariadb(mock_build_connector, mock_connector):
    """Test getting a MariaDB adapter."""
    connector, connection, cursor = mock_connector
    mock_build_connector.return_value = connector

    config = Config(
        project_path=Path("/fake/path"),
        host="localhost",
        port=3306,
        instance="",
        database="test_db",
        username="test_user",
        password="test_password",
        trusted_auth=False,
        sql_dialect="mariadb",
        default_schema="public",
        log_table="sqlstride_log",
        lock_table="sqlstride_lock",
        jinja_vars={}
    )

    adapter = get_adapter(config)

    # Check that the connector was built with the correct config
    mock_build_connector.assert_called_once_with(config)

    # Check that the correct connection method was called
    connector.to_user_mysql.assert_called_once()

    # Check that the adapter has the correct attributes
    assert adapter.default_schema == "public"
    assert adapter.log_table == "sqlstride_log"
    assert adapter.lock_table == "sqlstride_lock"


@patch("sqlstride.adapters.mssql.build_connector")
def test_get_adapter_mssql(mock_build_connector, mock_connector):
    """Test getting a MSSQL adapter."""
    connector, connection, cursor = mock_connector
    mock_build_connector.return_value = connector

    config = Config(
        project_path=Path("/fake/path"),
        host="localhost",
        port=1433,
        instance="SQLEXPRESS",
        database="test_db",
        username="test_user",
        password="test_password",
        trusted_auth=True,
        sql_dialect="mssql",
        default_schema="dbo",
        log_table="sqlstride_log",
        lock_table="sqlstride_lock",
        jinja_vars={}
    )

    adapter = get_adapter(config)

    # Check that the connector was built with the correct config
    mock_build_connector.assert_called_once_with(config)

    # Check that the correct connection method was called
    connector.to_trusted_msql.assert_called_once()

    # Check that the adapter has the correct attributes
    assert adapter.default_schema == "dbo"
    assert adapter.log_table == "sqlstride_log"
    assert adapter.lock_table == "sqlstride_lock"


@patch("sqlstride.connector_proxy.build_connector")
def test_get_adapter_invalid_dialect(mock_build_connector):
    """Test getting an adapter with an invalid dialect."""
    config = Config(
        project_path=Path("/fake/path"),
        host="localhost",
        port=5432,
        instance="",
        database="test_db",
        username="test_user",
        password="test_password",
        trusted_auth=False,
        sql_dialect="invalid",
        default_schema="public",
        log_table="sqlstride_log",
        lock_table="sqlstride_lock",
        jinja_vars={}
    )

    with pytest.raises(ValueError, match="Unsupported SQL dialect"):
        get_adapter(config)
