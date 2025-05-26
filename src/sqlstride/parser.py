# sqlstride/parser.py
from pathlib import Path
from typing import List, NamedTuple
from .constants import STEP_PATTERN, ORDERED_DIRS
from etl.logger import Logger

logger = Logger().get_logger()

__all__ = ["Step", "parse_sql_file", "parse_directory"]


class Step(NamedTuple):
    author: str
    step_id: str
    sql: str
    filename: str


def parse_sql_file(file_path: Path, base_dir: Path) -> List[Step]:
    steps: List[Step] = []
    content = file_path.read_text(encoding="utf-8")

    matches = list(STEP_PATTERN.finditer(content))
    logger.debug(f"Found {len(matches)} steps in {file_path}")
    for i, match in enumerate(matches):
        author, step_id = match.groups()
        start = match.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(content)
        sql_block = content[start:end].strip()
        relative_name = file_path.relative_to(base_dir).as_posix()
        steps.append(
            Step(author=author, step_id=step_id, sql=sql_block, filename=relative_name)
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
                all_steps.extend(parse_sql_file(file_path, directory))

    # 2. any other subdirectories, in alphabetical order
    for subdir in sorted(
        path for path in directory.iterdir() if path.is_dir() and path.name not in handled
    ):
        for file_path in _sql_files_in(subdir):
            all_steps.extend(parse_sql_file(file_path, directory))
    logger.debug(f"Found {len(all_steps)} steps in project directory")
    return all_steps

