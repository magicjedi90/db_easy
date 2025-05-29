import os
import tempfile
from pathlib import Path
import pytest
import yaml
from sqlstride.config import Config


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdirname:
        yield Path(tmpdirname)


@pytest.fixture
def sample_sql_file(temp_dir):
    """Create a sample SQL file with multiple steps."""
    file_path = temp_dir / "sample.sql"
    content = """
    -- step author1:step1
    CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        username VARCHAR(100) NOT NULL
    );
    
    -- step author1:step2
    ALTER TABLE users ADD COLUMN email VARCHAR(255);
    """
    file_path.write_text(content)
    return file_path


@pytest.fixture
def sample_jinja_sql_file(temp_dir):
    """Create a sample SQL file with Jinja templating."""
    file_path = temp_dir / "sample.sql.j2"
    content = """
    -- step author1:step1
    CREATE TABLE {{ schema_prefix }}users (
        id SERIAL PRIMARY KEY,
        username VARCHAR(100) NOT NULL
    );
    
    {% if environment == 'development' %}
    -- step author1:step2
    INSERT INTO {{ schema_prefix }}users (username) VALUES ('test_user');
    {% endif %}
    """
    file_path.write_text(content)
    return file_path


@pytest.fixture
def sample_project_structure(temp_dir):
    """Create a sample project structure with multiple directories and SQL files."""
    # Create directory structure
    for directory in ["tables", "functions", "views"]:
        os.makedirs(temp_dir / directory, exist_ok=True)
    
    # Create sample SQL files
    (temp_dir / "tables" / "users.sql").write_text("""
    -- step author1:create_users
    CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        username VARCHAR(100) NOT NULL
    );
    """)
    
    (temp_dir / "functions" / "get_user.sql").write_text("""
    -- step author1:create_get_user_function
    CREATE FUNCTION get_user(user_id INT) RETURNS TABLE (
        id INT,
        username VARCHAR(100)
    ) AS $$
        SELECT id, username FROM users WHERE id = user_id;
    $$ LANGUAGE SQL;
    """)
    
    (temp_dir / "views" / "active_users.sql").write_text("""
    -- step author1:create_active_users_view
    CREATE VIEW active_users AS
    SELECT * FROM users WHERE last_login > NOW() - INTERVAL '30 days';
    """)
    
    return temp_dir


@pytest.fixture
def sample_config_file(temp_dir):
    """Create a sample configuration file."""
    config_path = temp_dir / "sqlstride.yaml"
    config_data = {
        "sql_dialect": "postgres",
        "host": "localhost",
        "port": 5432,
        "database": "test_db",
        "username": "test_user",
        "password": "test_password",
        "default_schema": "public",
        "log_table": "sqlstride_log",
        "lock_table": "sqlstride_lock",
        "jinja_vars": {
            "environment": "development",
            "schema_prefix": "test_"
        }
    }
    
    with open(config_path, 'w') as f:
        yaml.dump(config_data, f)
    
    return config_path


@pytest.fixture
def mock_config():
    """Create a mock configuration object."""
    return Config(
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
        jinja_vars={"environment": "development", "schema_prefix": "test_"}
    )