# sqlstride/templating.py
from jinja2 import Environment, BaseLoader

_env = Environment(loader=BaseLoader(), autoescape=False)


def render_sql(sql_text: str, vars_: dict, filename: str) -> str:
    """Render *.sql.j2 files with Jinja2; return raw SQL unchanged otherwise."""
    if filename.endswith(".j2"):
        return _env.from_string(sql_text).render(**vars_)
    return sql_text
