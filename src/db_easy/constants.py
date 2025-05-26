import re

# Detect lines like  -- step author:id
STEP_PATTERN = re.compile(r"--\s*step\s+(\w+):(\w+)", re.IGNORECASE)
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
