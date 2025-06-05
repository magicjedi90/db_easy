import os
from sqlstride.file_utils.parser import parse_sql_file, parse_directory, Step
from sqlstride.constants import ORDERED_DIRS


def test_parse_sql_file(sample_sql_file, temp_dir):
    """Test parsing a SQL file with multiple steps."""
    steps = parse_sql_file(sample_sql_file, temp_dir)

    assert len(steps) == 2

    # Check first step
    assert steps[0].author == "author1"
    assert steps[0].step_id == "step1"
    assert "CREATE TABLE users" in steps[0].sql
    assert steps[0].filename == "sample.sql"

    # Check second step
    assert steps[1].author == "author1"
    assert steps[1].step_id == "step2"
    assert "ALTER TABLE users" in steps[1].sql
    assert steps[1].filename == "sample.sql"


def test_parse_sql_file_empty(temp_dir):
    """Test parsing an empty SQL file."""
    file_path = temp_dir / "empty.sql"
    file_path.write_text("")

    steps = parse_sql_file(file_path, temp_dir)
    assert len(steps) == 0


def test_parse_sql_file_no_steps(temp_dir):
    """Test parsing a SQL file with no step markers."""
    file_path = temp_dir / "no_steps.sql"
    file_path.write_text("""
    CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        username VARCHAR(100) NOT NULL
    );
    """)

    steps = parse_sql_file(file_path, temp_dir)
    assert len(steps) == 0


def test_parse_directory(sample_project_structure):
    """Test parsing a directory with multiple SQL files in different subdirectories."""
    steps = parse_directory(sample_project_structure)

    # Check that we have the expected number of steps
    assert len(steps) == 3

    # Check that steps are in the correct order based on ORDERED_DIRS
    # Tables should come before functions and views
    table_steps = [s for s in steps if "tables" in s.filename]
    function_steps = [s for s in steps if "functions" in s.filename]
    view_steps = [s for s in steps if "views" in s.filename]

    # Check the order based on ORDERED_DIRS
    tables_index = ORDERED_DIRS.index("tables")
    functions_index = ORDERED_DIRS.index("functions")
    views_index = ORDERED_DIRS.index("views")

    # Verify the order is correct
    if tables_index < functions_index:
        assert steps.index(table_steps[0]) < steps.index(function_steps[0])
    if functions_index < views_index:
        assert steps.index(function_steps[0]) < steps.index(view_steps[0])


def test_parse_directory_empty(temp_dir):
    """Test parsing an empty directory."""
    steps = parse_directory(temp_dir)
    assert len(steps) == 0


def test_parse_directory_with_non_sql_files(temp_dir):
    """Test parsing a directory with non-SQL files."""
    # Create a non-SQL file
    (temp_dir / "not_sql.txt").write_text("This is not a SQL file")

    # Create a SQL file
    os.makedirs(temp_dir / "tables", exist_ok=True)
    (temp_dir / "tables" / "users.sql").write_text("""
    -- step author1:create_users
    CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        username VARCHAR(100) NOT NULL
    );
    """)

    steps = parse_directory(temp_dir)

    # Should only find the SQL file
    assert len(steps) == 1
    assert steps[0].step_id == "create_users"


def test_step_namedtuple():
    """Test the Step named tuple."""
    step = Step(author="test_author", step_id="test_step", sql="SELECT 1", filename="test.sql")

    assert step.author == "test_author"
    assert step.step_id == "test_step"
    assert step.sql == "SELECT 1"
    assert step.filename == "test.sql"
