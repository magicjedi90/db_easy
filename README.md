# SQLStride

Zero-boilerplate schema migrations — straight from your own SQL files.

SQLStride is a simple, yet powerful database migration tool that allows you to manage your database schema changes using
plain SQL files organized in a specific directory structure.

## Installation

```bash
pip install sqlstride
```

Requirements:

- Python 3.8 or higher
- Dependencies: etl-utilities, pymysql, click, pyodbc, pyyaml, jinja2

## Configuration

SQLStride uses a YAML configuration file named `sqlstride.yaml` (for backward compatibility) in your project root
directory. Here's an example of what
this file should look like:

```yaml
# Required
sql_dialect: postgres  # Options: postgres, mssql, mariadb
host: localhost

# Optional with no defaults
port: 5432
instance: mssql_db_instance
database: my_database
username: db_user
password: db_password
default_schema: public

# Optional with defaults
trusted_auth: false
log_table: sqlstride_log
lock_table: sqlstride_lock

# Jinja template variables
jinja_vars:
  environment: production
  schema_prefix: app_
  # Add any variables you want to use in your SQL templates
```

### Configuration Options

| Option         | Description                                         | Default        |
|----------------|-----------------------------------------------------|----------------|
| sql_dialect    | SQL dialect to use                                  | postgres       |
| host           | Database host (required)                            | -              |
| port           | Database port                                       | -              |
| database       | Database name                                       | -              |
| username       | Database username                                   | -              |
| password       | Database password                                   | -              |
| instance       | Instance name (for MSSQL)                           | -              |
| default_schema | Default schema the log and lock tables will save to | -              |
| trusted_auth   | Use trusted authentication (for MSSQL)              | false          |
| log_table      | Name of the log table                               | sqlstride_log  |
| lock_table     | Name of the lock table                              | sqlstride_lock |
| jinja_vars     | Variables to use in Jinja SQL templates             | {}             |

## Folder Structure and Execution Order

SQLStride executes SQL files in a specific order based on the directory structure. The tool processes directories in the
following order:

1. **Infrastructure/Runtime**
    - `extensions` (for modules)
    - `roles` (for users)

2. **Namespaces & Custom Types**
    - `schemas`
    - `types` (domains, enums, composite types)
    - `sequences`

3. **Core Relational Objects**
    - `tables`
    - `indexes`

4. **Reference/Seed Data**
    - `seed_data` (for data)

5. **Relational Constraints**
    - `constraints`

6. **Programmable Objects**
    - `functions`
    - `procedures`
    - `triggers`

7. **Wrapper/Presentation Objects**
    - `views`
    - `materialized_views`
    - `synonyms`

8. **Security & Automation**
    - `grants` (for permissions)
    - `jobs` (for tasks)

9. **Clean-up Scripts**
    - `retire`

Within each directory, SQL files are processed in alphabetical order.

## SQL File Format

Each SQL file can contain multiple migration steps. A step is identified by a special comment line:

```sql
-- step author:step_id
```

For example:

```sql
-- step john:create_users_table
CREATE TABLE users
(
    id         SERIAL PRIMARY KEY,
    username   VARCHAR(100) NOT NULL,
    email      VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMP    NOT NULL DEFAULT NOW()
);

-- step john:add_user_index
CREATE INDEX idx_users_username ON users (username);
```

## Jinja Templating

SQLStride supports Jinja2 templating in SQL files. To use this feature, name your SQL files with a `.j2` extension (
e.g., `users.sql.j2`). You can then use Jinja2 syntax in your SQL files:

```sql
-- step john:create_users_table
CREATE TABLE {{ schema_prefix }} users
(
    id
    SERIAL
    PRIMARY
    KEY,
    username
    VARCHAR
(
    100
) NOT NULL,
    email VARCHAR
(
    255
) NOT NULL UNIQUE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW
(
)
    );

{% if environment == 'development' %}
-- step john:add_test_user
INSERT INTO {{ schema_prefix }} users (username, email)
VALUES ('test_user', 'test@example.com');
{% endif %}
```

You can define variables in the `jinja_vars` section of your configuration file or pass them via the `--jinja-vars`
command-line option as a JSON string.

## CLI Usage

### Sync Command

The main command is `sqlstride sync`, which synchronizes your database schema with your SQL files.

```bash
sqlstride sync [OPTIONS]
```

### Create Repository Structure

The `create_repo` command creates the recommended directory structure for your project with blank `__init__.py` files in
each directory. It also interactively prompts you for configuration values to generate a customized configuration file.

```bash
sqlstride create_repo [OPTIONS]
```

### Sync Command Options

| Option           | Description                                                                                           |
|------------------|-------------------------------------------------------------------------------------------------------|
| --project, -p    | Path to schema repo containing the configuration file & schema/ (default: current directory)          |
| --host           | Database host                                                                                         |
| --port           | Database port                                                                                         |
| --instance       | Instance used for connecting to MSSQL Database                                                        |
| --database, -db  | Desired database to connect to on host                                                                |
| --username, -u   | Username used for authenticating with the database                                                    |
| --password, -pw  | Password used for authenticating with the database                                                    |
| --trusted-auth   | Use trusted authentication for connecting to MSSQL Database                                           |
| --sql-dialect    | SQL dialect to use for connecting to database                                                         |
| --default-schema | Schema that the log and lock tables will be created in                                                |
| --log-table      | Name of the table to use to keep track of changes                                                     |
| --lock-table     | Name of the table to use to lock the database during sync                                             |
| --dry-run        | Parse & list SQL without executing anything                                                           |
| --same-checksums | Checks the current checksums against the existing checksums and raises an error if they are different |
| --jinja-vars     | JSON string of variables to use in Jinja templates                                                    |

### Create Repository Structure Options

| Option        | Description                                                          |
|---------------|----------------------------------------------------------------------|
| --project, -p | Path to create the repository structure (default: current directory) |

## Examples

### Basic Usage

```bash
# Navigate to your project directory containing the configuration file
cd my_project

# Run the sync command
sqlstride sync

# Create the repository structure in the current directory
sqlstride create_repo
```

### Using Command-line Options

```bash
# Override configuration options
sqlstride sync --host db.example.com --port 5432 --database my_db --username admin --password secret

# Dry run to see what would be executed without making changes
sqlstride sync --dry-run

# Check if any applied migrations have changed
sqlstride sync --same-checksums

# Create repository structure in a specific directory
sqlstride create_repo --project /path/to/my_new_project

# Use Jinja template variables
sqlstride sync --jinja-vars '{"environment": "development", "schema_prefix": "dev_"}'
```

### Project Structure Example

```
my_project/
├── sqlstride.yaml  # Configuration file (name kept for backward compatibility)
├── schema/
├── tables/
│   ├── users.sql
│   └── posts.sql.j2  # Jinja template
├── constraints/
│   └── foreign_keys.sql
├── functions/
│   └── user_functions.sql
└── views/
  └── user_posts_view.sql
```

## How It Works

1. SQLStride scans your project directory for SQL files in the specified order.
2. It parses each file to extract migration steps.
3. For files with a `.j2` extension, it processes them with Jinja2 templating using the provided variables.
4. It checks which steps have already been applied to the database.
5. It applies any pending steps in the correct order.
6. It records each applied step in the log table with a checksum to ensure idempotency.

This approach allows you to manage your database schema using plain SQL files without having to write boilerplate
migration code, with the added flexibility of using templates when needed.
