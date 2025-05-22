# db_easy/executor.py
from hashlib import sha256
from pathlib import Path

from .parser import parse_directory
from .templating import render_sql
from .adapters import get_adapter


def sync_database(config, *, dry_run: bool = False) -> None:
    adapter = get_adapter(config)
    if adapter.is_locked():
        raise Exception("db-easy is already running")
    adapter.lock()
    all_steps = parse_directory(Path(config.schema_path))
    applied = adapter.get_applied_steps()
    pending = [step for step in all_steps if (step.filename, step.author, step.step_id) not in applied]

    if not pending:
        print("✔ Database is already up to date.")
        return

    for step in pending:
        sql_rendered = render_sql(step.sql, config.template_vars, step.filename)
        checksum = sha256(sql_rendered.encode()).hexdigest()

        if dry_run:
            print(f"\n-- WOULD APPLY {step.author}:{step.step_id} ({step.filename})")
            print(sql_rendered)
            continue

        try:
            adapter.execute(sql_rendered)
            adapter.record_step(step, checksum)
            adapter.unlock()
            adapter.commit()
            print(f"✓ Applied {step.author}:{step.step_id}")
        except Exception as exc:
            adapter.rollback()
            raise RuntimeError(
                f"Failed on {step.author}:{step.step_id} → {exc}"
            ) from exc
