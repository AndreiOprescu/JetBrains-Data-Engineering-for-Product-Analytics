"""SQL execution backends.

All SQL in ``sql/`` is written in the **Trino** dialect, because production
runs on Trino against Iceberg tables. Two engines execute it:

* :class:`TrinoEngine` - production. Statements are sent as written.
* :class:`DuckDBEngine` - local demo / tests. Statements are transpiled
  Trino -> DuckDB with sqlglot, so the *same* SQL files are exercised and
  there is a single source of truth for the transformation logic.
"""

from __future__ import annotations

import logging
import os
import re
from typing import Any, Iterable, Protocol, Sequence

logger = logging.getLogger(__name__)

_PLACEHOLDER = re.compile(r"\{\{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\}\}")


def render(sql: str, params: dict[str, Any] | None = None) -> str:
    """Substitute ``{{ name }}`` placeholders; fail loudly on missing ones."""
    params = params or {}

    def repl(match: re.Match[str]) -> str:
        key = match.group(1)
        if key not in params:
            raise KeyError(f"SQL placeholder '{{{{ {key} }}}}' has no value")
        value = str(params[key])
        if "'" in value:
            raise ValueError(f"Refusing to render a quote into SQL: {key}={value!r}")
        return value

    return _PLACEHOLDER.sub(repl, sql)


def split_statements(sql: str) -> list[str]:
    """Split a script on ``;`` while respecting quotes and ``--`` comments."""
    statements: list[str] = []
    buf: list[str] = []
    i, n = 0, len(sql)
    in_single = in_double = in_comment = False
    while i < n:
        ch = sql[i]
        if in_comment:
            buf.append(ch)
            if ch == "\n":
                in_comment = False
        elif in_single:
            buf.append(ch)
            if ch == "'":
                in_single = False
        elif in_double:
            buf.append(ch)
            if ch == '"':
                in_double = False
        elif ch == "-" and sql.startswith("--", i):
            in_comment = True
            buf.append(ch)
        elif ch == "'":
            in_single = True
            buf.append(ch)
        elif ch == '"':
            in_double = True
            buf.append(ch)
        elif ch == ";":
            statements.append("".join(buf))
            buf = []
        else:
            buf.append(ch)
        i += 1
    statements.append("".join(buf))

    def has_code(stmt: str) -> bool:
        return any(line.strip() and not line.strip().startswith("--") for line in stmt.splitlines())

    return [s.strip() for s in statements if has_code(s)]


class Engine(Protocol):
    name: str

    def execute(self, sql: str) -> list[tuple]: ...

    def query(self, sql: str) -> tuple[list[str], list[tuple]]: ...

    def close(self) -> None: ...


def run_script(engine: Engine, sql: str, params: dict[str, Any] | None = None) -> None:
    for statement in split_statements(render(sql, params)):
        engine.execute(statement)


# --------------------------------------------------------------------------- #
# Trino (production)
# --------------------------------------------------------------------------- #
class TrinoEngine:
    """Executes SQL on a Trino cluster. Configuration via environment:

    TRINO_HOST, TRINO_PORT (8080), TRINO_USER (svc_meta_dwh), TRINO_CATALOG
    (iceberg), TRINO_HTTP_SCHEME (http|https), TRINO_PASSWORD (optional,
    enables basic auth).
    """

    name = "trino"

    def __init__(self, **overrides: Any) -> None:
        import trino  # imported lazily: not needed for the local demo

        cfg = {
            "host": os.environ.get("TRINO_HOST", "localhost"),
            "port": int(os.environ.get("TRINO_PORT", "8080")),
            "user": os.environ.get("TRINO_USER", "svc_meta_dwh"),
            "catalog": os.environ.get("TRINO_CATALOG", "iceberg"),
            "http_scheme": os.environ.get("TRINO_HTTP_SCHEME", "http"),
            # tag our own queries so they can be excluded from / identified in usage stats
            "source": "meta-dwh-pipeline",
            "client_tags": ["meta_dwh"],
        }
        password = os.environ.get("TRINO_PASSWORD")
        if password:
            cfg["auth"] = trino.auth.BasicAuthentication(cfg["user"], password)
        cfg.update(overrides)
        self._conn = trino.dbapi.connect(**cfg)

    def execute(self, sql: str) -> list[tuple]:
        cur = self._conn.cursor()
        cur.execute(sql)
        return cur.fetchall()  # also waits for DML to finish

    def query(self, sql: str) -> tuple[list[str], list[tuple]]:
        cur = self._conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        return [d[0] for d in (cur.description or [])], rows

    def close(self) -> None:
        self._conn.close()


# --------------------------------------------------------------------------- #
# DuckDB (local demo / tests)
# --------------------------------------------------------------------------- #
class DuckDBEngine:
    """Runs the Trino SQL on DuckDB through sqlglot.

    Dialect gaps handled here (and only here):
    * Iceberg table properties (``WITH (partitioning = ...)``) are dropped.
    * Trino expands ``UNNEST(array_of_rows) AS t(a, b, c)`` into columns
      a, b, c and the SQL always references them qualified (``t.a``).
      DuckDB yields a single struct column instead, so the alias is rewritten
      to ``AS _u_t(t)``; ``t.a`` then resolves to the struct field.
    """

    name = "duckdb"

    def __init__(self, path: str = ":memory:") -> None:
        import duckdb

        logging.getLogger("sqlglot").setLevel(logging.ERROR)
        self._conn = duckdb.connect(path)
        self._conn.execute("SET TimeZone = 'UTC'")

    @staticmethod
    def translate(sql: str) -> str:
        import sqlglot
        from sqlglot import exp

        tree = sqlglot.parse_one(sql, read="trino")

        def fix_unnest(node: exp.Expression) -> exp.Expression:
            if isinstance(node, exp.Unnest):
                alias = node.args.get("alias")
                if isinstance(alias, exp.TableAlias) and len(alias.columns) > 1 and alias.this:
                    name = alias.this.name
                    node.set(
                        "alias",
                        exp.TableAlias(
                            this=exp.to_identifier(f"_u_{name}"),
                            columns=[exp.to_identifier(name)],
                        ),
                    )
            return node

        tree = tree.transform(fix_unnest)
        return tree.sql(dialect="duckdb")

    def execute(self, sql: str) -> list[tuple]:
        translated = self.translate(sql)
        try:
            cur = self._conn.execute(translated)
        except Exception as exc:  # pragma: no cover - surfaced with context
            raise RuntimeError(f"DuckDB failed on:\n{translated}\n\n{exc}") from exc
        try:
            return cur.fetchall()
        except Exception:
            return []

    def query(self, sql: str) -> tuple[list[str], list[tuple]]:
        cur = self._conn.execute(self.translate(sql))
        rows = cur.fetchall()
        return [d[0] for d in cur.description], rows

    def native_query(self, sql: str) -> list[tuple]:
        """Run DuckDB SQL as-is (tests/debugging only)."""
        return self._conn.execute(sql).fetchall()

    def load_arrow(self, table: str, arrow_table: Any) -> None:
        """Bulk append an Arrow table (used by the sample-data generator)."""
        self._conn.register("_arrow_src", arrow_table)
        try:
            cols = ", ".join(arrow_table.column_names)
            self._conn.execute(f"INSERT INTO {table} ({cols}) SELECT {cols} FROM _arrow_src")
        finally:
            self._conn.unregister("_arrow_src")

    def close(self) -> None:
        self._conn.close()


def make_engine(kind: str, duckdb_path: str = "meta_dwh.duckdb") -> Engine:
    if kind == "trino":
        return TrinoEngine()
    if kind == "duckdb":
        return DuckDBEngine(duckdb_path)
    raise ValueError(f"unknown engine {kind!r}")


def format_rows(columns: Sequence[str], rows: Iterable[Sequence[Any]], max_rows: int = 50) -> str:
    """Render a small result set as an aligned text table."""

    def fmt(v: Any) -> str:
        if v is None:
            return "NULL"
        if isinstance(v, float):
            return f"{v:,.2f}"
        return str(v)

    data = [[fmt(v) for v in r] for r in list(rows)[:max_rows]]
    widths = [max([len(c)] + [len(r[i]) for r in data]) for i, c in enumerate(columns)]
    line = "  ".join(c.ljust(w) for c, w in zip(columns, widths))
    sep = "  ".join("-" * w for w in widths)
    body = ["  ".join(v.rjust(w) if _numeric(v) else v.ljust(w) for v, w in zip(r, widths)) for r in data]
    return "\n".join([line, sep, *body])


def _numeric(v: str) -> bool:
    return bool(re.fullmatch(r"-?[\d,]+(\.\d+)?", v))
