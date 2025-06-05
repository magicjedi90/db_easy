import pytest
from sqlstride.file_utils.templating import render_sql


def test_render_sql_no_template():
    """Test rendering SQL without templating."""
    sql = "SELECT * FROM users WHERE id = 1;"
    vars_ = {"table": "users", "id": 1}
    filename = "query.sql"
    
    result = render_sql(sql, vars_, filename)
    
    # Should return the SQL unchanged
    assert result == sql


def test_render_sql_with_template():
    """Test rendering SQL with Jinja templating."""
    sql = "SELECT * FROM {{ table }} WHERE id = {{ id }};"
    vars_ = {"table": "users", "id": 1}
    filename = "query.sql.j2"
    
    result = render_sql(sql, vars_, filename)
    
    # Should render the template
    assert result == "SELECT * FROM users WHERE id = 1;"


def test_render_sql_with_conditional():
    """Test rendering SQL with conditional Jinja templating."""
    sql = """
    SELECT * FROM users;
    {% if include_admin %}
    SELECT * FROM admins;
    {% endif %}
    """
    vars_ = {"include_admin": True}
    filename = "query.sql.j2"
    
    result = render_sql(sql, vars_, filename)
    
    # Should include the admin query
    assert "SELECT * FROM admins;" in result
    
    # Test with include_admin = False
    vars_ = {"include_admin": False}
    result = render_sql(sql, vars_, filename)
    
    # Should not include the admin query
    assert "SELECT * FROM admins;" not in result


def test_render_sql_with_loop():
    """Test rendering SQL with loop in Jinja templating."""
    sql = """
    {% for table in tables %}
    SELECT * FROM {{ table }};
    {% endfor %}
    """
    vars_ = {"tables": ["users", "orders", "products"]}
    filename = "query.sql.j2"
    
    result = render_sql(sql, vars_, filename)
    
    # Should include all tables
    assert "SELECT * FROM users;" in result
    assert "SELECT * FROM orders;" in result
    assert "SELECT * FROM products;" in result


def test_render_sql_missing_variable():
    """Test rendering SQL with missing variable."""
    sql = "SELECT * FROM {{ table }} WHERE id = {{ id }};"
    vars_ = {"table": "users"}  # Missing 'id'
    filename = "query.sql.j2"
    
    # Should raise an exception
    with pytest.raises(Exception):
        render_sql(sql, vars_, filename)


def test_render_sql_with_sample_jinja_file(sample_jinja_sql_file, temp_dir):
    """Test rendering SQL with a sample Jinja SQL file."""
    # Read the content of the sample file
    sql = sample_jinja_sql_file.read_text()
    vars_ = {"environment": "development", "schema_prefix": "test_"}
    filename = sample_jinja_sql_file.name
    
    result = render_sql(sql, vars_, filename)
    
    # Check that the template was rendered correctly
    assert "CREATE TABLE test_users" in result
    assert "INSERT INTO test_users" in result
    
    # Test with different environment
    vars_ = {"environment": "production", "schema_prefix": "prod_"}
    result = render_sql(sql, vars_, filename)
    
    # Should not include the insert statement in production
    assert "CREATE TABLE prod_users" in result
    assert "INSERT INTO prod_users" not in result