# sqlstride/database/`````````````````````````````````````````database_object.py
from dataclasses import dataclass
from pathlib import Path

@dataclass
class DatabaseObject:
    kind: str        # table | view | function | …
    schema: str
    name: str
    ddl: str

    def default_path(self, project_root: Path) -> Path:
        """
        {project_root}/{kind}s/{schema}/{object}.sql
        →  tables/public/users.sql
        """
        return (
            project_root
            / f"{self.kind}s"
            / self.schema
            / f"{self.name}.sql"
        )