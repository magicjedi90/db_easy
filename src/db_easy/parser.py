# db_easy/parser.py
import re
from pathlib import Path
from typing import List, NamedTuple

__all__ = ["Step", "parse_sql_file", "parse_directory"]

# Detect lines like  -- step author:id
STEP_PATTERN = re.compile(r"--\\s*step\\s+(\\w+):(\\w+)", re.IGNORECASE)
# execution order for sub-directories
ORDERED_DIRS = [
    # 1. infrastructure / runtime
    "extensions",        # or "modules"
    "roles",             # or "users"

    # 2. namespaces & custom types
    "schemas",
    "types",             # domains / enums / composite types
    "sequences",

    # 3. core relational objects
    "tables",
    "indexes",

    # 4. reference / seed data (before FKs turn on)
    "seed_data",         # or simply "data"

    # 5. relational constraints that depend on tables & data
    "constraints",

    # 6. programmable objects
    "functions",
    "procedures",      # or "procedures"
    "triggers",

    # 7. wrapper / presentation objects
    "views",
    "materialized_views",
    "synonyms",

    # 8. security & automation
    "grants",            # or "permissions"
    "jobs",              # or "tasks"

    # 9. clean-up scripts
    "retire",
]

class Step(NamedTuple):
    author: str
    step_id: str
    sql: str
    filename: str


def parse_sql_file(file_path: Path) -> List[Step]:
    steps: List[Step] = []
    content = file_path.read_text(encoding="utf-8")

    matches = list(STEP_PATTERN.finditer(content))
    for i, match in enumerate(matches):
        author, step_id = match.groups()
        start = match.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(content)
        sql_block = content[start:end].strip()
        steps.append(
            Step(author=author, step_id=step_id, sql=sql_block, filename=file_path.name)
        )
    return steps


def _sql_files_in(dir_: Path) -> List[Path]:
    """
    Helper: return all *.sql / *.sql.* files in the given directory,
    sorted alphabetically.
    """
    return sorted(dir_.rglob("*.sql*"))


def parse_directory(directory: Path) -> List[Step]:
    """
    Walk the directory and its immediate sub-directories in a
    deterministic order so SQL runs safely in dependency order.

    1. Pre-defined folders in ORDERED_DIRS (if they exist)
    2. Any remaining folders, alphabetically
    """
    all_steps: List[Step] = []

    handled = set()
    for name in ORDERED_DIRS:
        subdir = directory / name
        if subdir.is_dir():
            handled.add(subdir.name)
            for file_path in _sql_files_in(subdir):
                all_steps.extend(parse_sql_file(file_path))

    # 2. any other subdirectories, in alphabetical order
    for subdir in sorted(
        path for path in directory.iterdir() if path.is_dir() and path.name not in handled
    ):
        for file_path in _sql_files_in(subdir):
            all_steps.extend(parse_sql_file(file_path))

    return all_steps

