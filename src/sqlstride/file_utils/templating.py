# sqlstride/templating.py
from jinja2 import Environment, BaseLoader, StrictUndefined, UndefinedError

# Raise an exception whenever an undefined variable is encountered
_env = Environment(
    loader=BaseLoader(),
    autoescape=False,
    undefined=StrictUndefined,       # <- key line
)


def render_sql(sql_text: str, vars_: dict, filename: str) -> str:
    """
    Render *.sql.j2 files with Jinja2.
    If a variable referenced in the template is missing,
    a ValueError is raised with a helpful message.
    """
    if not filename.endswith(".j2"):
        return sql_text

    try:
        template = _env.from_string(sql_text)
        return template.render(**vars_)
    except UndefinedError as exc:
        # Provide a clearer, domain-specific error message
        raise ValueError(
            f"Template '{filename}' failed to render: missing Jinja variable – {exc}"
        ) from exc
