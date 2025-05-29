import pytest
import os
from pathlib import Path
import yaml
from sqlstride.config import load_config, Config


def test_load_config_from_file(sample_config_file, temp_dir):
    """Test loading configuration from a file."""
    config = load_config(
        project_path=temp_dir,
        host=None,
        port=None,
        instance=None,
        database=None,
        username=None,
        password=None,
        trusted_auth=False,
        sql_dialect=None,
        default_schema=None
    )
    
    assert config.host == "localhost"
    assert config.port == 5432
    assert config.database == "test_db"
    assert config.username == "test_user"
    assert config.password == "test_password"
    assert config.default_schema == "public"
    assert config.sql_dialect == "postgres"
    assert config.log_table == "sqlstride_log"
    assert config.lock_table == "sqlstride_lock"
    assert config.jinja_vars == {"environment": "development", "schema_prefix": "test_"}


def test_load_config_with_cli_overrides(sample_config_file, temp_dir):
    """Test loading configuration with CLI overrides."""
    config = load_config(
        project_path=temp_dir,
        host="override_host",
        port=1234,
        instance="override_instance",
        database="override_db",
        username="override_user",
        password="override_password",
        trusted_auth=True,
        sql_dialect="mariadb",
        default_schema="override_schema",
        log_table="override_log",
        lock_table="override_lock",
        jinja_vars={"environment": "production"}
    )
    
    # CLI options should override file options
    assert config.host == "override_host"
    assert config.port == 1234
    assert config.instance == "override_instance"
    assert config.database == "override_db"
    assert config.username == "override_user"
    assert config.password == "override_password"
    assert config.trusted_auth is True
    assert config.sql_dialect == "mariadb"
    assert config.default_schema == "override_schema"
    assert config.log_table == "override_log"
    assert config.lock_table == "override_lock"
    assert config.jinja_vars == {"environment": "production"}


def test_load_config_missing_file(temp_dir):
    """Test loading configuration with a missing file."""
    with pytest.raises(FileNotFoundError):
        load_config(
            project_path=temp_dir,
            host=None,
            port=None,
            instance=None,
            database=None,
            username=None,
            password=None,
            trusted_auth=False,
            sql_dialect=None,
            default_schema=None
        )


def test_load_config_missing_host(temp_dir):
    """Test loading configuration with a missing host."""
    # Create a config file without a host
    config_path = temp_dir / "sqlstride.yaml"
    config_data = {
        "sql_dialect": "postgres",
        # No host
        "port": 5432,
        "database": "test_db"
    }
    
    with open(config_path, 'w') as f:
        yaml.dump(config_data, f)
    
    # Should raise ValueError because host is required
    with pytest.raises(ValueError, match="host is required"):
        load_config(
            project_path=temp_dir,
            host=None,
            port=None,
            instance=None,
            database=None,
            username=None,
            password=None,
            trusted_auth=False,
            sql_dialect=None,
            default_schema=None
        )


def test_config_dataclass():
    """Test the Config dataclass."""
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
        jinja_vars={"environment": "development"}
    )
    
    assert config.host == "localhost"
    assert config.port == 5432
    assert config.database == "test_db"
    assert config.username == "test_user"
    assert config.password == "test_password"
    assert config.default_schema == "public"
    assert config.sql_dialect == "postgres"
    assert config.log_table == "sqlstride_log"
    assert config.lock_table == "sqlstride_lock"
    assert config.jinja_vars == {"environment": "development"}