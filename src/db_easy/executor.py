# db_easy/executor.py
from hashlib import sha256
from pathlib import Path

from .parser import parse_directory
from .templating import render_sql
from .adapters import get_adapter


def sync_database(config, *, dry_run: bool = False) -> None:
    # db = Database(cfg.db_url, cfg.log_table)
    adapter = get_adapter(config)
    all_steps = parse_directory(Path(config.schema_path))
    applied = db.applied_steps()
    pending = [s for s in all_steps if (s.author, s.step_id) not in applied]

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

        with db.connect() as conn:
            try:
                conn.execute(sql_rendered)
                db.record_step(conn, step, checksum)
                conn.commit()
                print(f"✓ Applied {step.author}:{step.step_id}")
            except Exception as exc:
                conn.rollback()
                raise RuntimeError(
                    f"Failed on {step.author}:{step.step_id} → {exc}"
                ) from exc
