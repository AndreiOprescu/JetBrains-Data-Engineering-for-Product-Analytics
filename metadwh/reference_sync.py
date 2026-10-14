"""Load reference metadata (teams, memberships, ownership, ...) into meta_ref.

In production these tables are fed from systems of record (IdP group sync,
data catalog, FinOps config) by a small sync job; here they are read from the
CSV files in ``reference_data/`` which have exactly the target structure.
Each table is fully replaced: reference data is small and the source is the
truth, so a full snapshot is simpler and safer than a diff.
"""

from __future__ import annotations

import csv
from pathlib import Path

from .engine import Engine

# column -> SQL type, in table column order
REFERENCE_TABLES: dict[str, dict[str, str]] = {
    "team": {
        "team_id": "varchar", "team_name": "varchar", "department": "varchar",
        "cost_center": "varchar", "is_active": "boolean",
    },
    "user_team_membership": {
        "user_name": "varchar", "team_id": "varchar", "valid_from": "date", "valid_to": "date",
    },
    "user_account": {
        "user_name": "varchar", "account_type": "varchar", "display_name": "varchar",
        "is_active": "boolean",
    },
    "table_ownership": {
        "catalog_name": "varchar", "schema_name": "varchar", "table_name": "varchar",
        "owner_team_id": "varchar", "data_domain": "varchar",
    },
    "catalog": {"catalog_name": "varchar", "connector_type": "varchar", "is_data_lake": "boolean"},
    "cost_rate": {
        "valid_from": "date", "valid_to": "date", "cpu_hour_usd": "double",
        "memory_gb_hour_usd": "double", "scanned_tb_usd": "double",
    },
}


def _literal(value: str | None, sql_type: str) -> str:
    if value is None or value.strip() == "":
        return "NULL"
    value = value.strip()
    if sql_type == "boolean":
        return "TRUE" if value.lower() in ("true", "1", "yes", "y") else "FALSE"
    if sql_type == "double":
        return f"CAST({float(value)!r} AS DOUBLE)"
    escaped = value.replace("'", "''")
    if sql_type == "date":
        return f"DATE '{escaped}'"
    return f"'{escaped}'"


def sync_reference_data(engine: Engine, directory: str | Path) -> dict[str, int]:
    directory = Path(directory)
    loaded: dict[str, int] = {}
    for table, columns in REFERENCE_TABLES.items():
        path = directory / f"{table}.csv"
        if not path.exists():
            raise FileNotFoundError(path)
        with path.open(newline="", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        missing = set(columns) - set(rows[0].keys() if rows else columns)
        if missing:
            raise ValueError(f"{path}: missing columns {sorted(missing)}")

        engine.execute(f"DELETE FROM meta_ref.{table}")
        for start in range(0, len(rows), 500):
            values = ",\n".join(
                "(" + ", ".join(_literal(r.get(c), t) for c, t in columns.items()) + ")"
                for r in rows[start:start + 500]
            )
            engine.execute(f"INSERT INTO meta_ref.{table} ({', '.join(columns)}) VALUES {values}")
        loaded[table] = len(rows)
    return loaded
