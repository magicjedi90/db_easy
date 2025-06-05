import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path
from sqlstride.commands.sync import sync_database
from sqlstride.commands.create_repo import create_repository_structure
from sqlstride.file_utils.parser import Step


@pytest.fixture
def mock_adapter():
    """Create a mock adapter."""
    adapter = MagicMock()
    adapter.is_locked.return_value = False
    adapter.applied_steps.return_value = {}
    return adapter


@pytest.fixture
def mock_steps():
    """Create mock steps."""
    return [
        Step(author="author1", step_id="step1", sql="SELECT 1", filename="file1.sql"),
        Step(author="author2", step_id="step2", sql="SELECT 2", filename="file2.sql")
    ]


@patch("sqlstride.commands.get_adapter")
@patch("sqlstride.commands.parse_directory")
@patch("sqlstride.commands.render_sql")
def test_sync_database_no_pending_steps(mock_render_sql, mock_parse_directory, mock_get_adapter, mock_adapter, mock_config, capsys):
    """Test syncing the database with no pending steps."""
    mock_get_adapter.return_value = mock_adapter
    mock_parse_directory.return_value = []
    
    sync_database(mock_config)
    
    # Check that the adapter was created
    mock_get_adapter.assert_called_once_with(mock_config)
    
    # Check that the directory was parsed
    mock_parse_directory.assert_called_once_with(Path(mock_config.project_path))
    
    # Check that applied steps were checked
    mock_adapter.applied_steps.assert_called_once()
    
    # Check that the success message was printed
    captured = capsys.readouterr()
    assert "Database is already up to date" in captured.out


@patch("sqlstride.commands.get_adapter")
@patch("sqlstride.commands.parse_directory")
@patch("sqlstride.commands.render_sql")
def test_sync_database_with_pending_steps(mock_render_sql, mock_parse_directory, mock_get_adapter, mock_adapter, mock_steps, mock_config, capsys):
    """Test syncing the database with pending steps."""
    mock_get_adapter.return_value = mock_adapter
    mock_parse_directory.return_value = mock_steps
    mock_render_sql.side_effect = lambda sql, vars_, filename: sql  # Just return the SQL unchanged
    
    sync_database(mock_config)
    
    # Check that the adapter was created
    mock_get_adapter.assert_called_once_with(mock_config)
    
    # Check that the directory was parsed
    mock_parse_directory.assert_called_once_with(Path(mock_config.project_path))
    
    # Check that applied steps were checked
    mock_adapter.applied_steps.assert_called_once()
    
    # Check that each step was executed
    assert mock_adapter.lock.call_count == 2
    assert mock_adapter.execute.call_count == 2
    assert mock_adapter.record_step.call_count == 2
    assert mock_adapter.unlock.call_count == 2
    assert mock_adapter.commit.call_count == 2
    
    # Check that the success messages were printed
    captured = capsys.readouterr()
    assert "Applied file1.sql author1:step1" in captured.out
    assert "Applied file2.sql author2:step2" in captured.out


@patch("sqlstride.commands.get_adapter")
@patch("sqlstride.commands.parse_directory")
@patch("sqlstride.commands.render_sql")
def test_sync_database_dry_run(mock_render_sql, mock_parse_directory, mock_get_adapter, mock_adapter, mock_steps, mock_config, capsys):
    """Test syncing the database in dry run mode."""
    mock_get_adapter.return_value = mock_adapter
    mock_parse_directory.return_value = mock_steps
    mock_render_sql.side_effect = lambda sql, vars_, filename: sql  # Just return the SQL unchanged
    
    sync_database(mock_config, dry_run=True)
    
    # Check that the adapter was created
    mock_get_adapter.assert_called_once_with(mock_config)
    
    # Check that the directory was parsed
    mock_parse_directory.assert_called_once_with(Path(mock_config.project_path))
    
    # Check that applied steps were checked
    mock_adapter.applied_steps.assert_called_once()
    
    # Check that no steps were executed
    mock_adapter.lock.assert_not_called()
    mock_adapter.execute.assert_not_called()
    mock_adapter.record_step.assert_not_called()
    mock_adapter.unlock.assert_not_called()
    mock_adapter.commit.assert_not_called()
    
    # Check that the dry run messages were printed
    captured = capsys.readouterr()
    assert "WOULD APPLY author1:step1" in captured.out
    assert "WOULD APPLY author2:step2" in captured.out


@patch("sqlstride.commands.get_adapter")
@patch("sqlstride.commands.parse_directory")
@patch("sqlstride.commands.render_sql")
def test_sync_database_same_checksums(mock_render_sql, mock_parse_directory, mock_get_adapter, mock_adapter, mock_config):
    """Test syncing the database with same_checksums=True."""
    mock_get_adapter.return_value = mock_adapter
    
    # Create steps that have already been applied
    steps = [
        Step(author="author1", step_id="step1", sql="SELECT 1", filename="file1.sql"),
        Step(author="author2", step_id="step2", sql="SELECT 2", filename="file2.sql")
    ]
    mock_parse_directory.return_value = steps
    
    # Mock applied steps with matching checksums
    mock_adapter.applied_steps.return_value = {
        ("author1", "step1", "file1.sql"): "checksum1",
        ("author2", "step2", "file2.sql"): "checksum2"
    }
    
    # Mock render_sql to return checksums that match the applied steps
    mock_render_sql.side_effect = lambda sql, vars_, filename: sql
    
    # Mock sha256 to return the expected checksums
    with patch("sqlstride.commands.sha256") as mock_sha256:
        mock_sha256().hexdigest.side_effect = ["checksum1", "checksum2"]
        
        # Should not raise an exception
        sync_database(mock_config, same_checksums=True)


@patch("sqlstride.commands.get_adapter")
@patch("sqlstride.commands.parse_directory")
@patch("sqlstride.commands.render_sql")
def test_sync_database_different_checksums(mock_render_sql, mock_parse_directory, mock_get_adapter, mock_adapter, mock_config):
    """Test syncing the database with same_checksums=True and different checksums."""
    mock_get_adapter.return_value = mock_adapter
    
    # Create steps that have already been applied
    steps = [
        Step(author="author1", step_id="step1", sql="SELECT 1", filename="file1.sql"),
        Step(author="author2", step_id="step2", sql="SELECT 2", filename="file2.sql")
    ]
    mock_parse_directory.return_value = steps
    
    # Mock applied steps with different checksums
    mock_adapter.applied_steps.return_value = {
        ("author1", "step1", "file1.sql"): "old_checksum1",
        ("author2", "step2", "file2.sql"): "old_checksum2"
    }
    
    # Mock render_sql to return the SQL unchanged
    mock_render_sql.side_effect = lambda sql, vars_, filename: sql
    
    # Mock sha256 to return different checksums
    with patch("sqlstride.commands.sha256") as mock_sha256:
        mock_sha256().hexdigest.side_effect = ["new_checksum1", "new_checksum2"]
        
        # Should raise an exception
        with pytest.raises(Exception, match="Checksums for the following steps are different"):
            sync_database(mock_config, same_checksums=True)


@patch("sqlstride.commands.get_adapter")
def test_sync_database_already_locked(mock_get_adapter, mock_adapter, mock_config):
    """Test syncing the database when it's already locked."""
    mock_adapter.is_locked.return_value = True
    mock_get_adapter.return_value = mock_adapter
    
    with pytest.raises(Exception, match="sqlstride is already running"):
        sync_database(mock_config)


@patch("sqlstride.commands.get_adapter")
@patch("sqlstride.commands.parse_directory")
@patch("sqlstride.commands.render_sql")
def test_sync_database_execution_error(mock_render_sql, mock_parse_directory, mock_get_adapter, mock_adapter, mock_steps, mock_config):
    """Test syncing the database with an execution error."""
    mock_get_adapter.return_value = mock_adapter
    mock_parse_directory.return_value = mock_steps
    mock_render_sql.side_effect = lambda sql, vars_, filename: sql  # Just return the SQL unchanged
    
    # Make the execute method raise an exception
    mock_adapter.execute.side_effect = Exception("SQL error")
    
    with pytest.raises(RuntimeError, match="Failed on file1.sql author1:step1"):
        sync_database(mock_config)
    
    # Check that rollback was called
    mock_adapter.rollback.assert_called_once()


@patch("sqlstride.commands.click")
@patch("sqlstride.commands.os.makedirs")
@patch("builtins.open")
def test_create_repository_structure(mock_open, mock_makedirs, mock_click):
    """Test creating the repository structure."""
    # Mock user input
    mock_click.prompt.side_effect = [
        "postgres",  # SQL dialect
        "localhost",  # Host
        "5432",  # Port
        "test_db",  # Database
        "",  # Instance
        False,  # Trusted auth
        "test_user",  # Username
        "test_password",  # Password
        "public",  # Default schema
        "sqlstride_log",  # Log table
        "sqlstride_lock"  # Lock table
    ]
    mock_click.confirm.return_value = False
    
    # Mock file operations
    mock_file = MagicMock()
    mock_open.return_value.__enter__.return_value = mock_file
    
    # Call the function
    create_repository_structure("/fake/path")
    
    # Check that directories were created
    assert mock_makedirs.call_count > 0
    
    # Check that the config file was written
    assert mock_open.call_count > 0
    assert mock_file.write.call_count > 0
    
    # Check that the success message was printed
    mock_click.echo.assert_called()