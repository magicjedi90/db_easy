# db_easy/parser.py
import re
from pathlib import Path
from typing import List, NamedTuple

__all__ = ["Step", "parse_sql_file", "parse_directory"]

# Detect lines like  -- step author:id
STEP_PATTERN = re.compile(r"--\\s*step\\s+(\\w+):(\\w+)", re.IGNORECASE)


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


def parse_directory(directory: Path) -> List[Step]:
    all_steps: List[Step] = []
    for file_path in sorted(directory.glob("*.sql*")):  # *.sql and *.sql.j2
        all_steps.extend(parse_sql_file(file_path))
    return all_steps
