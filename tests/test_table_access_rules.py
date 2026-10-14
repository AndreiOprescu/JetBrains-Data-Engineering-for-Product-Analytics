"""Unit-level tests of the multi-table attribution rules, on hand-made events."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from metadwh.engine import DuckDBEngine, split_statements
from metadwh.pipeline import init, run_backfill
from metadwh.reference_sync import sync_reference_data
from metadwh.sample_data import load_events

ROOT = Path(__file__).resolve().parent.parent
DAY = date(2026, 9, 10)
T0 = datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc)


def inp(fqn: str, nbytes: int | None, columns=("a", "b")):
    catalog, schema, table = fqn.split(".")
    return {"catalog_name": catalog, "schema_name": schema, "table_name": table, "columns": list(columns),
            "physical_input_bytes": nbytes, "physical_input_rows": 10}


def event(query_id: str, inputs, output=None, *, user="anna.kowalski", cpu=1000, state="FINISHED",
          ingested_delta=timedelta(seconds=5)):
    out = None
    if output:
        catalog, schema, table = output.split(".")
        out = {"catalog_name": catalog, "schema_name": schema, "table_name": table, "columns": ["x"]}
    return {
        "query_id": query_id, "query_state": state, "query_type": "INSERT" if output else "SELECT",
        "user_name": user, "principal": user, "source": "trino-cli", "client_tags": [],
        "create_time": T0 - timedelta(seconds=10), "execution_start_time": T0 - timedelta(seconds=9),
        "end_time": T0, "queued_time_ms": 100, "wall_time_ms": 9000, "cpu_time_ms": cpu,
        "peak_total_memory_bytes": 1000, "cumulative_memory_byte_seconds": 500.0,
        "physical_input_bytes": sum(i["physical_input_bytes"] or 0 for i in inputs),
        "written_bytes": 77 if output else 0, "written_rows": 7 if output else 0,
        "inputs": inputs, "output_table": out, "ingested_at": T0 + ingested_delta,
    }


@pytest.fixture(scope="module")
def access():
    engine = DuckDBEngine(":memory:")
    init(engine, with_raw=True)
    sync_reference_data(engine, ROOT / "reference_data")
    load_events(engine, [
        event("join", [inp("lake.events.clicks", 300), inp("lake.product.users", 100)]),
        event("selfjoin", [inp("lake.events.clicks", 200, ["a"]), inp("lake.events.clicks", 200, ["b", "c"])]),
        event("meta", [inp("lake.events.page_views$snapshots", 10)]),
        event("federated", [inp("crm.public.accounts", 0), inp("lake.product.licenses", 400)]),
        event("nobytes", [inp("crm.public.accounts", 0), inp("crm.public.opportunities", None)]),
        event("ctas", [inp("lake.events.page_views", 100)], output="lake.events.sessions"),
        event("values", [], output="lake.ml.recommendations"),
        event("select1", []),
        event("read_write_same", [inp("lake.events.sessions", 50)], output="lake.events.sessions"),
        # duplicate delivery with different content: the later copy wins
        event("dup", [inp("lake.events.clicks", 1)], cpu=1),
        event("dup", [inp("lake.events.clicks", 1)], cpu=2, ingested_delta=timedelta(minutes=5)),
    ])
    run_backfill(engine, DAY, DAY)
    cols, data = engine.query("SELECT * FROM meta_dwh.fct_table_access")
    result = {}
    for r in data:
        rec = dict(zip(cols, r))
        result[(rec["query_id"], rec["table_fqn"], rec["access_type"])] = rec
    return engine, result


def w(access, q, t, a="READ"):
    return access[1][(q, t, a)]["allocation_weight"]


def test_weights_follow_bytes_share(access):
    assert w(access, "join", "lake.events.clicks") == pytest.approx(0.75)
    assert w(access, "join", "lake.product.users") == pytest.approx(0.25)
    assert access[1][("join", "lake.events.clicks", "READ")]["allocated_cpu_time_ms"] == pytest.approx(750)


def test_self_join_is_collapsed(access):
    rec = access[1][("selfjoin", "lake.events.clicks", "READ")]
    assert rec["allocation_weight"] == pytest.approx(1)
    assert rec["physical_input_bytes"] == 400
    assert sorted(rec["columns_accessed"]) == ["a", "b", "c"]
    assert len([k for k in access[1] if k[0] == "selfjoin"]) == 1


def test_iceberg_metadata_table_maps_to_base_table(access):
    rec = access[1][("meta", "lake.events.page_views", "READ")]
    assert rec["is_metadata_table_only"] is True


def test_federated_source_without_bytes(access):
    assert w(access, "federated", "crm.public.accounts") == pytest.approx(0)
    assert w(access, "federated", "lake.product.licenses") == pytest.approx(1)
    # nobody reports bytes -> equal split
    assert w(access, "nobytes", "crm.public.accounts") == pytest.approx(0.5)
    assert w(access, "nobytes", "crm.public.opportunities") == pytest.approx(0.5)


def test_writes(access):
    assert w(access, "ctas", "lake.events.sessions", "WRITE") == 0
    assert w(access, "ctas", "lake.events.page_views") == 1
    assert access[1][("ctas", "lake.events.sessions", "WRITE")]["written_bytes"] == 77
    assert w(access, "values", "lake.ml.recommendations", "WRITE") == 1
    assert w(access, "read_write_same", "lake.events.sessions", "READ") == 1
    assert w(access, "read_write_same", "lake.events.sessions", "WRITE") == 0


def test_query_without_tables_only_in_fct_query(access):
    engine, result = access
    assert not [k for k in result if k[0] == "select1"]
    assert engine.query("SELECT COUNT(*) FROM meta_dwh.fct_query WHERE query_id = 'select1'")[1] == [(1,)]


def test_read_write_same_table_counted_once_in_table_daily(access):
    engine, _ = access
    row = engine.query("SELECT query_count, read_query_count, write_query_count FROM meta_dwh.agg_table_daily "
                       "WHERE table_fqn = 'lake.events.sessions'")[1][0]
    assert row == (2, 1, 2)  # ctas (write) + read_write_same (read & write)


def test_last_delivered_duplicate_wins(access):
    engine, _ = access
    assert engine.query("SELECT cpu_time_ms FROM meta_dwh.fct_query WHERE query_id = 'dup'")[1] == [(2,)]


def test_split_statements_respects_quotes_and_comments():
    sql = "SELECT 'a;b' AS x; -- comment; with semicolon\nSELECT 2;\n-- trailing comment only"
    assert split_statements(sql) == ["SELECT 'a;b' AS x", "-- comment; with semicolon\nSELECT 2"]


def test_unnest_translation():
    sql = "SELECT i.a FROM q CROSS JOIN UNNEST(q.arr) AS i(a, b)"
    assert "AS _u_i(i)" in DuckDBEngine.translate(sql)
