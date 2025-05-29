# SQLStride Tests

This directory contains unit tests for the SQLStride project.

## Running Tests

To run the tests, you need to have pytest installed:

```bash
pip install pytest
```

Then, from the project root directory, run:

```bash
pytest
```

Or to run a specific test file:

```bash
pytest tests/test_parser.py
```

Or to run a specific test function:

```bash
pytest tests/test_parser.py::test_parse_sql_file
```

## Test Coverage

To run the tests with coverage reporting, you need to have pytest-cov installed:

```bash
pip install pytest-cov
```

Then, from the project root directory, run:

```bash
pytest --cov=sqlstride
```

This will show a coverage report in the terminal. For a more detailed HTML report:

```bash
pytest --cov=sqlstride --cov-report=html
```

This will create a `htmlcov` directory with an HTML report that you can open in a web browser.

## Test Structure

The tests are organized by module:

- `test_parser.py`: Tests for the parser module
- `test_config.py`: Tests for the config module
- `test_templating.py`: Tests for the templating module
- `test_adapters.py`: Tests for the adapters module
- `test_executor.py`: Tests for the executor module
- `test_cli.py`: Tests for the CLI module

## Test Fixtures

Common test fixtures are defined in `conftest.py`. These include:

- `temp_dir`: Creates a temporary directory for test files
- `sample_sql_file`: Creates a sample SQL file with multiple steps
- `sample_jinja_sql_file`: Creates a sample SQL file with Jinja templating
- `sample_project_structure`: Creates a sample project structure with multiple directories and SQL files
- `sample_config_file`: Creates a sample configuration file
- `mock_config`: Creates a mock configuration object
- `mock_connection`: Creates a mock database connection
- `mock_connector`: Creates a mock connector that returns a mock connection
- `mock_adapter`: Creates a mock adapter
- `mock_steps`: Creates mock steps