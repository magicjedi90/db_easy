# DB-Easy

Zero-boilerplate schema migrations — straight from your own SQL files.

DB-Easy is a simple, yet powerful database migration tool that allows you to manage your database schema changes using
plain SQL files organized in a specific directory structure.

## Installation

```bash
pip install db-easy
```

Requirements:

- Python 3.8 or higher
- Dependencies: etl-utilities, pymysql, click, pyodbc

## Configuration

DB-Easy uses a YAML configuration file named `db-easy.yaml` in your project root directory. Here's an example of what
this file should look like:

```yaml
# Required
host: localhost
port: 5432
database: my_database
username: db_user
password: db_password

# Optional with defaults
sql_dialect: postgres  # Options: postgres, mssql, mariadb
default_schema: public
trusted_auth: false
log_table: db_easy_log
lock_table: db_easy_lock
```

### Configuration Options

| Option         | Description                                         | Default      |
|----------------|-----------------------------------------------------|--------------|
| host           | Database host (required)                            | -            |
| port           | Database port                                       | -            |
| database       | Database name                                       | -            |
| username       | Database username                                   | -            |
| password       | Database password                                   | -            |
| instance       | Instance name (for MSSQL)                           | -            |
| sql_dialect    | SQL dialect to use                                  | postgres     |
| default_schema | Default schema the log and lock tables will save to | -            |
| trusted_auth   | Use trusted authentication (for MSSQL)              | false        |
| log_table      | Name of the log table                               | db_easy_log  |
| lock_table     | Name of the lock table                              | db_easy_lock |

## Folder Structure and Execution Order

DB-Easy executes SQL files in a specific order based on the directory structure. The tool processes directories in the
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

## CLI Usage

The main command is `db-easy sync`, which synchronizes your database schema with your SQL files.

```bash
db-easy sync [OPTIONS]
```

### Options

| Option           | Description                                                                                           |
|------------------|-------------------------------------------------------------------------------------------------------|
| --project, -p    | Path to schema repo containing db-easy.yaml & schema/ (default: current directory)                    |
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

## Examples

### Basic Usage

```bash
# Navigate to your project directory containing db-easy.yaml
cd my_project

# Run the sync command
db-easy sync
```

### Using Command-line Options

```bash
# Override configuration options
db-easy sync --host db.example.com --port 5432 --database my_db --username admin --password secret

# Dry run to see what would be executed without making changes
db-easy sync --dry-run

# Check if any applied migrations have changed
db-easy sync --same-checksums
```

### Project Structure Example

```
my_project/
├── db-easy.yaml
├── schema/
├── tables/
│   ├── users.sql
│   └── posts.sql
├── constraints/
│   └── foreign_keys.sql
├── functions/
│   └── user_functions.sql
└── views/
  └── user_posts_view.sql
```

## How It Works

1. DB-Easy scans your project directory for SQL files in the specified order.
2. It parses each file to extract migration steps.
3. It checks which steps have already been applied to the database.
4. It applies any pending steps in the correct order.
5. It records each applied step in the log table with a checksum to ensure idempotency.

This approach allows you to manage your database schema using plain SQL files without having to write boilerplate
migration code.