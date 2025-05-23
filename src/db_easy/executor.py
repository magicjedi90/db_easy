# db_easy/executor.py
from hashlib import sha256
from pathlib import Path

from .parser import parse_directory
from .templating import render_sql
from .adapters import get_adapter


def sync_database(config, *, dry_run: bool = False, same_checksums: bool = False) -> None:
    adapter = get_adapter(config)
    if adapter.is_locked():
        raise Exception("db-easy is already running")
    all_steps = parse_directory(Path(config.schema_path))
    applied = adapter.get_applied_steps()
    if same_checksums:
        different_checksums = []
        #  if applied checksums are different from new checksums raise error
        already_applied = [step for step in all_steps if (step.filename, step.author, step.step_id) in applied]
        for step in already_applied:
            sql_rendered = render_sql(step.sql, config.template_vars, step.filename)
            checksum = sha256(sql_rendered.encode()).hexdigest()
            if checksum != applied[step.filename, step.author, step.step_id]:
                different_checksums.append((step.filename, step.author, step.step_id))
        if different_checksums:
            different_checksum_string = "\n".join(
                f"{filename} {author}:{step_id}" for filename, author, step_id in different_checksums)
            raise Exception(f"Checksums for the following steps are different:\n{different_checksum_string}")

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
            adapter.lock()
            adapter.execute(sql_rendered)
            adapter.record_step(step, checksum)
            adapter.unlock()
            adapter.commit()
            print(f"✓ Applied {step.filename} {step.author}:{step.step_id}")
        except Exception as exc:
            adapter.rollback()
            raise RuntimeError(
                f"Failed on {step.filename} {step.author}:{step.step_id} → {exc}"
            ) from exc
