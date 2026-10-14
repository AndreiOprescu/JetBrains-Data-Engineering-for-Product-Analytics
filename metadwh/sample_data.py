"""Synthetic Trino QueryCompletedEvents for the local demo and the tests.

The generator produces events shaped like ``raw_trino.query_completed`` and
deliberately includes the awkward cases the pipeline must handle:

* multi-table joins, cross-catalog (Iceberg + PostgreSQL) federation,
  INSERT ... SELECT, CTAS-like writes and pure INSERT ... VALUES
* self-joins (same table listed twice in ``inputs``)
* Iceberg metadata tables (``page_views$snapshots``)
* queries touching no table (``SELECT 1``, ``SHOW TABLES``) and system tables
* failed queries (analysis errors without inputs, resource errors with inputs)
* users who change team, an unmapped contractor, impersonation via Superset
* duplicate deliveries and late-arriving events (ingested days later)
* a usage trend (ML team grows) so that history/trend queries show something
"""

from __future__ import annotations

import random
import string
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

GB = 1024 ** 3
MB = 1024 ** 2


@dataclass(frozen=True)
class TableSpec:
    fqn: str
    typical_scan_gb: float           # median bytes read per query
    columns: tuple[str, ...]
    report_bytes: bool = True        # JDBC connectors report rows but 0 bytes


TABLES: dict[str, TableSpec] = {t.fqn: t for t in [
    TableSpec("lake.events.page_views", 15, ("event_time", "user_id", "url", "referrer", "country", "device")),
    TableSpec("lake.events.clicks", 6, ("event_time", "user_id", "element_id", "page_id")),
    TableSpec("lake.events.sessions", 8, ("session_id", "user_id", "start_time", "duration_s", "country")),
    TableSpec("lake.events.ide_usage", 30, ("event_time", "user_id", "ide", "ide_version", "feature", "action", "os")),
    TableSpec("lake.product.licenses", 0.8, ("license_id", "account_id", "product", "plan", "valid_until")),
    TableSpec("lake.product.users", 1.5, ("user_id", "account_id", "country", "created_at", "segment")),
    TableSpec("lake.product.subscriptions", 0.6, ("subscription_id", "account_id", "product", "mrr", "status")),
    TableSpec("lake.marketing.campaigns", 0.05, ("campaign_id", "channel", "budget", "start_date")),
    TableSpec("lake.marketing.attribution", 4, ("user_id", "campaign_id", "touch_time", "weight")),
    TableSpec("lake.finance.invoices", 0.9, ("invoice_id", "account_id", "amount", "currency", "issued_at")),
    TableSpec("lake.finance.revenue_daily", 0.02, ("day", "product", "region", "revenue_usd")),
    TableSpec("lake.ml.user_features", 4, ("user_id", "feature_vector", "updated_at")),
    TableSpec("lake.ml.training_set_v3", 8, ("user_id", "label", "features", "split")),
    TableSpec("lake.ml.recommendations", 3, ("user_id", "item_id", "score", "model_version")),
    TableSpec("lake.staging.tmp_sessionization", 5, ("session_id", "user_id", "event_time")),
    TableSpec("lake.sandbox.anna_tmp", 0.3, ("user_id", "x")),
    TableSpec("crm.public.accounts", 0.0, ("account_id", "name", "industry", "owner"), report_bytes=False),
    TableSpec("crm.public.opportunities", 0.0, ("opportunity_id", "account_id", "stage", "amount"), report_bytes=False),
    TableSpec("system.runtime.queries", 0.0, ("query_id", "state", "user", "query"), report_bytes=False),
]}

# Which tables each team mostly reads (weights).
TEAM_TABLES: dict[str, dict[str, float]] = {
    "data-platform": {"lake.events.page_views": 3, "lake.events.clicks": 2, "lake.events.sessions": 2,
                      "lake.staging.tmp_sessionization": 1, "lake.sandbox.anna_tmp": 0.5,
                      "system.runtime.queries": 0.7},
    "product-analytics": {"lake.events.ide_usage": 4, "lake.product.licenses": 3, "lake.product.users": 3,
                          "lake.events.sessions": 2, "lake.product.subscriptions": 1},
    "ml-recsys": {"lake.ml.user_features": 3, "lake.ml.training_set_v3": 2, "lake.ml.recommendations": 2,
                  "lake.events.ide_usage": 2, "lake.product.users": 1},
    "marketing-analytics": {"lake.marketing.attribution": 3, "lake.marketing.campaigns": 3,
                            "lake.events.page_views": 1.5, "lake.product.users": 1},
    "finance-bi": {"lake.finance.invoices": 3, "lake.finance.revenue_daily": 4,
                   "lake.product.subscriptions": 2, "lake.product.licenses": 1},
    "sales-ops": {"crm.public.accounts": 3, "crm.public.opportunities": 2, "lake.product.licenses": 2,
                  "lake.product.subscriptions": 1},
    "__unassigned__": {"lake.product.users": 1, "lake.events.sessions": 1},
}

# Human users: (name, team as used for *behaviour*, BI share). Team history in
# reference_data drives attribution; behaviour follows the same moves.
HUMANS = [
    ("anna.kowalski", "data-platform", 0.0), ("ben.martin", "data-platform", 0.0),
    ("chen.li", "data-platform", 0.1), ("daria.ivanova", "product-analytics", 0.5),
    ("emil.novak", "product-analytics", 0.3), ("fatima.zahra", "product-analytics", 0.6),
    ("george.white", "product-analytics", 0.4), ("hana.sato", "product-analytics", 0.5),
    ("ivan.petrov", "ml-recsys", 0.0), ("julia.schmidt", "ml-recsys", 0.1),
    ("kenji.mori", "ml-recsys", 0.0), ("lars.olsen", "marketing-analytics", 0.7),
    ("maria.garcia", "marketing-analytics", 0.6), ("nina.berg", "marketing-analytics", 0.5),
    ("oscar.lind", "finance-bi", 0.8), ("paula.costa", "finance-bi", 0.7),
    ("quentin.roux", "sales-ops", 0.5), ("rosa.mendes", "sales-ops", 0.4),
    ("ext.contractor1", "__unassigned__", 0.0),  # deliberately missing from reference data
]

TEAM_MOVES = {  # behaviour changes matching reference_data/user_team_membership.csv
    "emil.novak": (date(2026, 9, 1), "ml-recsys"),
    "hana.sato": (date(2026, 9, 15), "marketing-analytics"),
}

# Scheduled jobs: (user, source, output table, input tables, runs per day)
JOBS = [
    ("svc_airflow", "airflow", "lake.events.sessions", ["lake.events.page_views", "lake.events.clicks"], 6),
    ("svc_airflow", "airflow", "lake.staging.tmp_sessionization", ["lake.events.page_views"], 4),
    ("svc_dbt_finance", "dbt-trino", "lake.finance.revenue_daily",
     ["lake.finance.invoices", "lake.product.subscriptions"], 3),
    ("svc_ml_pipeline", "airflow", "lake.ml.training_set_v3",
     ["lake.ml.user_features", "lake.events.ide_usage", "lake.events.ide_usage"], 2),   # self-join
    ("svc_ml_pipeline", "airflow", "lake.ml.recommendations", [], 1),                   # INSERT ... VALUES
    ("svc_reverse_etl", "airflow", None, ["crm.public.accounts", "lake.product.licenses"], 8),
]

INTERACTIVE_SOURCES = ["trino-jdbc", "datagrip", "trino-cli", "jupyter", "trino-python-client"]


def _query_id(rng: random.Random, ts: datetime, seq: int) -> str:
    suffix = "".join(rng.choices(string.ascii_lowercase, k=5))
    return f"{ts:%Y%m%d_%H%M%S}_{seq:05d}_{suffix}"


def _team_of(user: str, team: str, day: date) -> str:
    move = TEAM_MOVES.get(user)
    if move and day >= move[0]:
        return move[1]
    return team


def _pick_tables(rng: random.Random, team: str, n: int) -> list[str]:
    weights = TEAM_TABLES[team]
    names = list(weights)
    chosen: list[str] = []
    for _ in range(n):
        t = rng.choices(names, weights=[weights[x] for x in names])[0]
        if t not in chosen:
            chosen.append(t)
    return chosen


def _event(
    rng: random.Random,
    *,
    seq: int,
    end_time: datetime,
    user: str,
    principal: str,
    source: str,
    inputs: list[str],
    output: str | None,
    query_type: str,
    metadata_suffix: str | None = None,
    fail: str | None = None,
) -> dict[str, Any]:
    input_rows: list[dict[str, Any]] = []
    total_bytes = 0
    total_rows = 0
    for idx, fqn in enumerate(inputs):
        spec = TABLES[fqn]
        catalog, schema, table = fqn.split(".")
        if metadata_suffix and idx == 0 and catalog == "lake":
            table = f"{table}${metadata_suffix}"
            scan = int(rng.uniform(0.1, 5) * MB)
        else:
            scan = int(spec.typical_scan_gb * GB * rng.lognormvariate(0, 0.8)) if spec.report_bytes else 0
        rows = max(1, scan // 120) if scan else rng.randint(100, 200_000)
        k = rng.randint(1, len(spec.columns))
        input_rows.append({
            "catalog_name": catalog, "schema_name": schema, "table_name": table,
            "columns": sorted(rng.sample(list(spec.columns), k)),
            "physical_input_bytes": scan, "physical_input_rows": rows,
        })
        total_bytes += scan
        total_rows += rows

    # Resource model: CPU follows bytes scanned, with noise and a fixed overhead.
    cpu_ms = int(200 + total_bytes / (40 * MB) * 1000 * rng.lognormvariate(0, 0.5))
    parallelism = rng.choice([4, 8, 16, 32, 64])
    wall_ms = int(cpu_ms / parallelism + rng.uniform(50, 1500))
    queued_ms = int(rng.expovariate(1 / 800)) if rng.random() < 0.7 else int(rng.uniform(5_000, 120_000))
    peak_mem = int(min(total_bytes * rng.uniform(0.02, 0.2), 200 * GB) + 64 * MB)
    state, error_code, error_type, failure_message = "FINISHED", None, None, None
    if fail == "analysis":
        state, error_code, error_type = "FAILED", "SYNTAX_ERROR", "USER_ERROR"
        failure_message = "line 1:8: mismatched input"
        input_rows, total_bytes, total_rows = [], 0, 0
        cpu_ms, wall_ms, peak_mem = rng.randint(1, 20), rng.randint(5, 60), 0
    elif fail == "resources":
        state, error_code, error_type = "FAILED", "EXCEEDED_USER_MEMORY_LIMIT", "INSUFFICIENT_RESOURCES"
        failure_message = "Query exceeded per-node memory limit"
        cpu_ms = int(cpu_ms * rng.uniform(0.2, 0.9))
        wall_ms = int(wall_ms * rng.uniform(0.2, 0.9))
    elif fail == "external":
        state, error_code, error_type = "FAILED", "ICEBERG_FILESYSTEM_ERROR", "EXTERNAL"
        failure_message = "S3 SlowDown"
        cpu_ms = int(cpu_ms * rng.uniform(0.05, 0.5))

    create_time = end_time - timedelta(milliseconds=wall_ms + queued_ms + rng.randint(20, 300))
    written_bytes = written_rows = 0
    output_row = None
    if output and state == "FINISHED":
        written_bytes = int(max(total_bytes, 50 * MB) * rng.uniform(0.02, 0.3))
        written_rows = max(1, written_bytes // 150)
    if output:
        catalog, schema, table = output.split(".")
        output_row = {"catalog_name": catalog, "schema_name": schema, "table_name": table,
                      "columns": list(TABLES[output].columns)}

    return {
        "query_id": _query_id(rng, end_time, seq),
        "query_state": state,
        "query_type": query_type,
        "update_type": "INSERT" if output else None,
        "query_text": f"-- synthetic {query_type} touching {', '.join(inputs) or 'no tables'}",
        "user_name": user,
        "principal": principal,
        "source": source,
        "client_tags": [],
        "resource_group_id": f"global.{'etl' if user.startswith('svc_') and user != 'svc_superset' else 'adhoc'}",
        "session_catalog": "lake",
        "session_schema": "default",
        "environment": "production",
        "server_version": "476",
        "create_time": create_time,
        "execution_start_time": create_time + timedelta(milliseconds=queued_ms),
        "end_time": end_time,
        "queued_time_ms": queued_ms,
        "analysis_time_ms": rng.randint(5, 300),
        "planning_time_ms": rng.randint(5, 500),
        "wall_time_ms": wall_ms,
        "cpu_time_ms": cpu_ms,
        "peak_user_memory_bytes": int(peak_mem * 0.9),
        "peak_total_memory_bytes": peak_mem,
        "cumulative_memory_byte_seconds": float(peak_mem * 0.6 * wall_ms / 1000),
        "physical_input_bytes": total_bytes,
        "physical_input_rows": total_rows,
        "processed_input_bytes": int(total_bytes * 1.4),
        "internal_network_bytes": int(total_bytes * rng.uniform(0.05, 0.5)),
        "output_bytes": 0 if output else rng.randint(100, 50 * MB),
        "output_rows": 0 if output else rng.randint(1, 100_000),
        "written_bytes": written_bytes,
        "written_rows": written_rows,
        "spilled_bytes": int(peak_mem * rng.uniform(0.5, 2)) if rng.random() < 0.03 else 0,
        "error_code": error_code,
        "error_type": error_type,
        "failure_message": failure_message,
        "inputs": input_rows,
        "output_table": output_row,
    }


def generate_events(start: date, days: int, seed: int = 42) -> list[dict[str, Any]]:
    """Generate raw events for ``days`` days starting at ``start`` (UTC)."""
    rng = random.Random(seed)
    events: list[dict[str, Any]] = []
    seq = 0

    for d in range(days):
        day = start + timedelta(days=d)
        weekend = day.weekday() >= 5
        midnight = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)

        # ---- scheduled jobs (every day, mostly at night) -------------------
        for user, source, output, inputs, runs in JOBS:
            for _ in range(runs):
                seq += 1
                end = midnight + timedelta(seconds=rng.randint(0, 6 * 3600) if rng.random() < 0.7
                                           else rng.randint(0, 86399))
                fail = "resources" if rng.random() < 0.02 else ("external" if rng.random() < 0.01 else None)
                qtype = "INSERT" if output else "SELECT"
                events.append(_event(rng, seq=seq, end_time=end, user=user, principal=user, source=source,
                                     inputs=list(inputs), output=output, query_type=qtype, fail=fail))

        # ---- humans --------------------------------------------------------
        for user, base_team, bi_share in HUMANS:
            team = _team_of(user, base_team, day)
            rate = rng.uniform(2, 8) if weekend else rng.uniform(12, 35)
            if team == "ml-recsys":               # usage trend: ML team ramps up over time
                rate *= 1 + d / max(days, 1)
            if weekend and rng.random() < 0.7:
                rate = 0
            for _ in range(int(rate)):
                seq += 1
                end = midnight + timedelta(seconds=int(rng.uniform(7, 19) * 3600 + rng.gauss(0, 3600)) % 86400)
                via_bi = rng.random() < bi_share
                source = "superset" if via_bi else rng.choice(INTERACTIVE_SOURCES)
                principal = "svc_superset" if via_bi else user
                r = rng.random()
                if r < 0.03:   # failed during analysis: no inputs
                    events.append(_event(rng, seq=seq, end_time=end, user=user, principal=principal,
                                         source=source, inputs=[], output=None, query_type="SELECT",
                                         fail="analysis"))
                    continue
                if r < 0.06:   # metadata-only statements
                    events.append(_event(rng, seq=seq, end_time=end, user=user, principal=principal,
                                         source=source, inputs=[], output=None,
                                         query_type=rng.choice(["DESCRIBE", "SELECT", "EXPLAIN"])))
                    continue
                n = rng.choices([1, 2, 3], weights=[0.55, 0.32, 0.13])[0]
                tables = _pick_tables(rng, team, n)
                metadata_suffix = None
                if team == "data-platform" and rng.random() < 0.08 and tables[0].startswith("lake."):
                    metadata_suffix = rng.choice(["snapshots", "files", "partitions"])
                fail = None
                if rng.random() < 0.025:
                    fail = "resources"
                elif rng.random() < 0.01:
                    fail = "external"
                output = None
                qtype = "SELECT"
                if team in ("data-platform", "ml-recsys") and not via_bi and rng.random() < 0.05:
                    output, qtype = "lake.sandbox.anna_tmp", "INSERT"
                events.append(_event(rng, seq=seq, end_time=end, user=user, principal=principal,
                                     source=source, inputs=tables, output=output, query_type=qtype,
                                     metadata_suffix=metadata_suffix, fail=fail))

    # ---- delivery semantics: ingestion lag, late events, duplicates ---------
    delivered: list[dict[str, Any]] = []
    for ev in events:
        lag = timedelta(seconds=rng.randint(2, 180))
        if rng.random() < 0.004:
            lag = timedelta(days=rng.randint(1, 4), seconds=rng.randint(0, 3600))
        ev["ingested_at"] = ev["end_time"] + lag
        delivered.append(ev)
        if rng.random() < 0.01:
            dup = dict(ev)
            dup["ingested_at"] = ev["ingested_at"] + timedelta(minutes=rng.randint(1, 90))
            delivered.append(dup)
    delivered.sort(key=lambda e: e["ingested_at"])
    return delivered


RAW_COLUMNS = [
    "query_id", "query_state", "query_type", "update_type", "query_text", "user_name", "principal",
    "source", "client_tags", "resource_group_id", "session_catalog", "session_schema", "environment",
    "server_version", "create_time", "execution_start_time", "end_time", "queued_time_ms",
    "analysis_time_ms", "planning_time_ms", "wall_time_ms", "cpu_time_ms", "peak_user_memory_bytes",
    "peak_total_memory_bytes", "cumulative_memory_byte_seconds", "physical_input_bytes",
    "physical_input_rows", "processed_input_bytes", "internal_network_bytes", "output_bytes",
    "output_rows", "written_bytes", "written_rows", "spilled_bytes", "error_code", "error_type",
    "failure_message", "inputs", "output_table", "ingested_at",
]


def to_arrow(events: list[dict[str, Any]]):
    """Convert events to an Arrow table matching raw_trino.query_completed."""
    import pyarrow as pa

    ts = pa.timestamp("us", tz="UTC")
    columns_t = pa.list_(pa.string())
    schema = pa.schema([
        ("query_id", pa.string()), ("query_state", pa.string()), ("query_type", pa.string()),
        ("update_type", pa.string()), ("query_text", pa.string()), ("user_name", pa.string()),
        ("principal", pa.string()), ("source", pa.string()), ("client_tags", columns_t),
        ("resource_group_id", pa.string()), ("session_catalog", pa.string()),
        ("session_schema", pa.string()), ("environment", pa.string()), ("server_version", pa.string()),
        ("create_time", ts), ("execution_start_time", ts), ("end_time", ts),
        ("queued_time_ms", pa.int64()), ("analysis_time_ms", pa.int64()), ("planning_time_ms", pa.int64()),
        ("wall_time_ms", pa.int64()), ("cpu_time_ms", pa.int64()), ("peak_user_memory_bytes", pa.int64()),
        ("peak_total_memory_bytes", pa.int64()), ("cumulative_memory_byte_seconds", pa.float64()),
        ("physical_input_bytes", pa.int64()), ("physical_input_rows", pa.int64()),
        ("processed_input_bytes", pa.int64()), ("internal_network_bytes", pa.int64()),
        ("output_bytes", pa.int64()), ("output_rows", pa.int64()), ("written_bytes", pa.int64()),
        ("written_rows", pa.int64()), ("spilled_bytes", pa.int64()), ("error_code", pa.string()),
        ("error_type", pa.string()), ("failure_message", pa.string()),
        ("inputs", pa.list_(pa.struct([
            ("catalog_name", pa.string()), ("schema_name", pa.string()), ("table_name", pa.string()),
            ("columns", columns_t), ("physical_input_bytes", pa.int64()), ("physical_input_rows", pa.int64()),
        ]))),
        ("output_table", pa.struct([
            ("catalog_name", pa.string()), ("schema_name", pa.string()), ("table_name", pa.string()),
            ("columns", columns_t),
        ])),
        ("ingested_at", ts),
    ])
    return pa.Table.from_pylist([{c: e.get(c) for c in RAW_COLUMNS} for e in events], schema=schema)


def _trino_literal(value: Any) -> str:
    """Trino SQL literal for the raw event fields (used when loading into Trino)."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return f"CAST({value!r} AS DOUBLE)"
    if isinstance(value, datetime):
        return f"TIMESTAMP '{value.astimezone(timezone.utc):%Y-%m-%d %H:%M:%S.%f} UTC'"
    if isinstance(value, list):
        if not value:
            return "ARRAY[]"
        return "ARRAY[" + ", ".join(_trino_literal(v) for v in value) + "]"
    if isinstance(value, dict):
        return "ROW(" + ", ".join(_trino_literal(v) for v in value.values()) + ")"
    return "'" + str(value).replace("'", "''") + "'"


def load_events(engine: Any, events: list[dict[str, Any]], batch_size: int = 200) -> int:
    """Append events to raw_trino.query_completed on either engine."""
    if not events:
        return 0
    if hasattr(engine, "load_arrow"):
        engine.load_arrow("raw_trino.query_completed", to_arrow(events))
        return len(events)
    # Trino: INSERT ... VALUES in batches (only meant for small demo volumes)
    cols = ", ".join(RAW_COLUMNS)
    for start in range(0, len(events), batch_size):
        values = ",\n".join(
            "(" + ", ".join(_trino_literal(e.get(c)) for c in RAW_COLUMNS) + ")"
            for e in events[start:start + batch_size]
        )
        engine.execute(f"INSERT INTO raw_trino.query_completed ({cols}) VALUES {values}")
    return len(events)
