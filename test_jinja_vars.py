from pathlib import Path
import sys
import json
sys.path.append("src")
from db_easy.config import Config
from db_easy.templating import render_sql

# Test that jinja_vars are correctly passed to the Config object
jinja_vars_dict = {"test_var": "test_value"}
config = Config(
    project_path=Path("."),
    host="localhost",
    port=5432,
    instance="",
    database="test_db",
    username="test_user",
    password="test_password",
    trusted_auth=False,
    sql_dialect="postgres",
    default_schema="public",
    jinja_vars=jinja_vars_dict
)

# Verify that jinja_vars are correctly set in the Config object
print(f"jinja_vars in Config: {config.jinja_vars}")

# Test that jinja_vars are correctly used in the templating system
sql_text = "SELECT * FROM {{ test_var }}"
filename = "test.sql.j2"
rendered_sql = render_sql(sql_text, config.jinja_vars, filename)
print(f"Rendered SQL: {rendered_sql}")

# Expected output: "Rendered SQL: SELECT * FROM test_value"
