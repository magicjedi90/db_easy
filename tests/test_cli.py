import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path
import json
from sqlstride.cli import cli, sync, create_repo


@patch("sqlstride.cli.load_config")
@patch("sqlstride.cli.sync_database")
def test_sync_command(mock_sync_database, mock_load_config, mock_config):
    """Test the sync command."""
    # Mock the load_config function to return our mock config
    mock_load_config.return_value = mock_config
    
    # Create a Click runner to invoke the command
    from click.testing import CliRunner
    runner = CliRunner()
    
    # Run the command with default options
    result = runner.invoke(sync, ["--project", "."])
    
    # Check that the command succeeded
    assert result.exit_code == 0
    
    # Check that load_config was called with the correct arguments
    mock_load_config.assert_called_once()
    args, kwargs = mock_load_config.call_args
    assert args[0] == Path(".")  # project_path
    
    # Check that sync_database was called with the correct arguments
    mock_sync_database.assert_called_once_with(mock_config, dry_run=False, same_checksums=False)


@patch("sqlstride.cli.load_config")
@patch("sqlstride.cli.sync_database")
def test_sync_command_with_options(mock_sync_database, mock_load_config, mock_config):
    """Test the sync command with various options."""
    # Mock the load_config function to return our mock config
    mock_load_config.return_value = mock_config
    
    # Create a Click runner to invoke the command
    from click.testing import CliRunner
    runner = CliRunner()
    
    # Run the command with various options
    result = runner.invoke(sync, [
        "--project", "/path/to/project",
        "--host", "db.example.com",
        "--port", "5432",
        "--database", "mydb",
        "--username", "user",
        "--password", "pass",
        "--sql-dialect", "postgres",
        "--default-schema", "public",
        "--dry-run",
        "--same-checksums",
        "--jinja-vars", '{"environment": "production"}'
    ])
    
    # Check that the command succeeded
    assert result.exit_code == 0
    
    # Check that load_config was called with the correct arguments
    mock_load_config.assert_called_once()
    args, kwargs = mock_load_config.call_args
    assert args[0] == Path("/path/to/project")  # project_path
    assert args[1] == "db.example.com"
    assert args[2] == "5432"
    assert args[3] is None
    assert args[4] == "mydb"
    assert args[5] == "user"
    assert args[6] == "pass"
    assert args[7] == False
    assert args[8] == "postgres"
    assert args[9] == "public"
    assert args[12] == {"environment": "production"}
    
    # Check that sync_database was called with the correct arguments
    mock_sync_database.assert_called_once_with(mock_config, dry_run=True, same_checksums=True)


@patch("sqlstride.cli.load_config")
@patch("sqlstride.cli.sync_database")
def test_sync_command_invalid_jinja_vars(mock_sync_database, mock_load_config):
    """Test the sync command with invalid JSON for jinja_vars."""
    # Create a Click runner to invoke the command
    from click.testing import CliRunner
    runner = CliRunner()
    
    # Run the command with invalid JSON for jinja_vars
    result = runner.invoke(sync, [
        "--project", ".",
        "--jinja-vars", "invalid-json"
    ])
    
    # Check that the command failed
    assert result.exit_code != 0
    
    # Check that the error message mentions jinja-vars
    assert "jinja-vars must be a valid JSON string" in result.output
    
    # Check that sync_database was not called
    mock_sync_database.assert_not_called()


@patch("sqlstride.cli.create_repository_structure")
def test_create_repo_command(mock_create_repository_structure):
    """Test the create_repo command."""
    # Create a Click runner to invoke the command
    from click.testing import CliRunner
    runner = CliRunner()
    
    # Run the command with default options
    result = runner.invoke(create_repo, ["--project", "."])
    
    # Check that the command succeeded
    assert result.exit_code == 0
    
    # Check that create_repository_structure was called with the correct arguments
    mock_create_repository_structure.assert_called_once_with(".")


@patch("sqlstride.cli.create_repository_structure")
def test_create_repo_command_with_path(mock_create_repository_structure):
    """Test the create_repo command with a custom path."""
    # Create a Click runner to invoke the command
    from click.testing import CliRunner
    runner = CliRunner()
    
    # Run the command with a custom path
    result = runner.invoke(create_repo, ["--project", "/path/to/project"])
    
    # Check that the command succeeded
    assert result.exit_code == 0
    
    # Check that create_repository_structure was called with the correct arguments
    mock_create_repository_structure.assert_called_once_with("/path/to/project")


def test_cli_group():
    """Test the CLI group."""
    # Create a Click runner to invoke the command
    from click.testing import CliRunner
    runner = CliRunner()
    
    # Run the CLI with --help to check that it works
    result = runner.invoke(cli, ["--help"])
    
    # Check that the command succeeded
    assert result.exit_code == 0
    
    # Check that the help text includes our commands
    assert "sync" in result.output
    assert "create-repo" in result.output