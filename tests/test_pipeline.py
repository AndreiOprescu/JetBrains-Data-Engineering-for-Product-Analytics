"""End-to-end tests of the Meta DWH transformations on DuckDB.

The SQL under test is the production Trino SQL, transpiled by sqlglot.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from metadwh.engine import DuckDBEngine, render, split_statements
from metadwh.pipeline import SQL_DIR, init, run_backfill, run_incremental
from metadwh.reference_sync import sync_reference_data
from metadwh.sample_data import generate_events, load_events

ROOT = Path(__file__).resolve().parent.parent
START = date(2026, 8, 20)
DAYS = 30
END = START + timedelta(days=DAYS - 1)
FAR_FUTURE = datetime(2030, 1, 1, tzinfo=timezone.utc)


def fresh_engine() -> DuckDBEngine:
    engine = DuckDBEngine(":memory:")
    init(engine, with_raw=True)
    sync_reference_data(engine, ROOT / "reference_data")
    return engine


@pytest.fixture(scope="module")
def events():
    return generate_events(START, DAYS, seed=7)


@pytest.fixture(scope="module")
def built(events):
    engine = fresh_engine()
    load_events(engine, events)
    run_incremental(engine, now=FAR_FUTURE)
    return engine


def rows(engine, sql):
    return engine.query(sql)[1]


def snapshot(engine):
    """Content of facts and aggregates, without load timestamps (DuckDB SQL)."""
    # floating point sums depend on aggregation order -> compare 10 significant digits
    def norm(v):
        return float(f"{v:.10g}") if isinstance(v, float) else v

    def rows(e, sql):
        return [tuple(norm(v) for v in r) for r in e.native_query(sql)]

    return {
        "fct_query": rows(engine, "SELECT * EXCLUDE (loaded_at) FROM meta_dwh.fct_query ORDER BY query_id"),
        "fct_table_access": rows(engine, "SELECT * EXCLUDE (loaded_at) FROM meta_dwh.fct_table_access "
                                         "ORDER BY query_id, table_fqn, access_type"),
        "agg_table_user_daily": rows(engine, "SELECT * FROM meta_dwh.agg_table_user_daily ORDER BY ALL"),
        "agg_table_daily": rows(engine, "SELECT * FROM meta_dwh.agg_table_daily ORDER BY ALL"),
        "agg_team_daily": rows(engine, "SELECT * FROM meta_dwh.agg_team_daily ORDER BY ALL"),
        "dim_table": rows(engine, "SELECT * EXCLUDE (updated_at) FROM meta_dwh.dim_table ORDER BY table_fqn"),
        "dim_user": rows(engine, "SELECT * EXCLUDE (updated_at) FROM meta_dwh.dim_user ORDER BY user_name"),
    }


# --------------------------------------------------------------------------- #
# correctness of the loaded model
# --------------------------------------------------------------------------- #
def test_data_quality_checks_pass(built):
    params = {"start_date": str(START), "end_date": str(END)}
    checks = (SQL_DIR / "checks/data_quality.sql").read_text()
    for statement in split_statements(render(checks, params)):
        if "-- check:" in statement:
            assert rows(built, statement) == [], statement.splitlines()[0]


def test_duplicates_are_removed(built, events):
    assert len({e["query_id"] for e in events}) < len(events), "generator should emit duplicates"
    n = rows(built, "SELECT COUNT(*), COUNT(DISTINCT query_id) FROM meta_dwh.fct_query")[0]
    assert n[0] == n[1] == len({e["query_id"] for e in events})


def test_point_in_time_team_attribution(built):
    teams = dict(rows(built, """
        SELECT user_name || '|' || CAST(query_date >= DATE '2026-09-01' AS VARCHAR),
               string_agg(DISTINCT team_id, ',')
        FROM meta_dwh.fct_query WHERE user_name = 'emil.novak' GROUP BY 1"""))
    assert teams == {"emil.novak|false": "product-analytics", "emil.novak|true": "ml-recsys"}

    # overlapping source intervals are cut: Hana is product-analytics until 09-15
    hana = dict(rows(built, """
        SELECT CAST(query_date >= DATE '2026-09-15' AS VARCHAR), string_agg(DISTINCT team_id, ',')
        FROM meta_dwh.fct_query WHERE user_name = 'hana.sato' GROUP BY 1"""))
    assert hana == {"false": "product-analytics", "true": "marketing-analytics"}

    assert rows(built, "SELECT DISTINCT team_id FROM meta_dwh.fct_query "
                       "WHERE user_name = 'ext.contractor1'") == [("__unassigned__",)]
    # dimension carries the CURRENT team for 'as of today' reporting
    assert rows(built, "SELECT current_team_id FROM meta_dwh.dim_user "
                       "WHERE user_name = 'emil.novak'") == [("ml-recsys",)]


def test_table_ownership_exact_beats_schema_default(built):
    owners = dict(rows(built, "SELECT table_fqn, owner_team_id FROM meta_dwh.dim_table"))
    assert owners["lake.events.ide_usage"] == "product-analytics"      # exact entry
    assert owners["lake.events.page_views"] == "data-platform"         # schema default
    assert owners["lake.sandbox.anna_tmp"] == "__unassigned__"         # no metadata
    assert owners["lake.legacy.events_2019"] == "data-platform"        # known, never queried


def test_distinct_users_example_matches_raw_events(built, events):
    table, lo, hi = "lake.events.ide_usage", date(2026, 9, 1), date(2026, 9, 9)
    latest = {}
    for e in events:  # dedupe like the pipeline: last delivered copy wins
        if e["query_id"] not in latest or e["ingested_at"] > latest[e["query_id"]]["ingested_at"]:
            latest[e["query_id"]] = e
    expected = {
        e["user_name"] for e in latest.values()
        if lo <= e["end_time"].date() <= hi
        and any(f'{i["catalog_name"]}.{i["schema_name"]}.{i["table_name"].split("$")[0]}' == table
                for i in e["inputs"])
    }
    sql = (SQL_DIR / "examples/01_distinct_users_of_table.sql").read_text()
    cols, result = built.query(render(sql, {"table_fqn": table, "start_date": lo, "end_date": hi}).rstrip(";\n"))
    assert result[0][cols.index("distinct_users")] == len(expected) > 0


def test_team_resource_example_matches_fact(built):
    sql = (SQL_DIR / "examples/02_team_resource_usage.sql").read_text()
    cols, result = built.query(render(sql, {"team_id": "ml-recsys", "start_date": "2026-09-01",
                                            "end_date": "2026-09-09"}).rstrip(";\n"))
    cpu_ms = rows(built, "SELECT SUM(cpu_time_ms) FROM meta_dwh.fct_query WHERE team_id = 'ml-recsys' "
                         "AND query_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-09'")[0][0]
    assert result[0][cols.index("cpu_hours")] == pytest.approx(cpu_ms / 3_600_000, abs=0.01)


@pytest.mark.parametrize("name", sorted(p.name for p in (SQL_DIR / "examples").glob("*.sql")))
def test_all_examples_run(built, name):
    params = {"table_fqn": "lake.events.page_views", "team_id": "data-platform", "start_date": str(START),
              "end_date": str(END), "as_of_date": str(END), "idle_days": "30"}
    for statement in split_statements(render((SQL_DIR / "examples" / name).read_text(), params)):
        built.query(statement)


# --------------------------------------------------------------------------- #
# update strategy: incremental == full rebuild, idempotency, backfills
# --------------------------------------------------------------------------- #
def test_incremental_batches_with_late_events_equal_full_rebuild(events, built):
    late = [e for e in events if e["ingested_at"] - e["end_time"] > timedelta(hours=12)]
    assert late, "generator should emit late events"

    engine = fresh_engine()
    cutoffs = [datetime(2026, 8, 27, tzinfo=timezone.utc), datetime(2026, 9, 3, 12, tzinfo=timezone.utc),
               datetime(2026, 9, 8, tzinfo=timezone.utc), FAR_FUTURE]
    lower = None
    for cutoff in cutoffs:
        batch = [e for e in events if (lower is None or e["ingested_at"] > lower) and e["ingested_at"] <= cutoff]
        load_events(engine, batch)
        run_incremental(engine, now=FAR_FUTURE)
        lower = cutoff
    assert snapshot(engine) == snapshot(built)


def test_incremental_run_without_new_data_does_nothing(built):
    assert run_incremental(built, now=FAR_FUTURE) == []


def test_backfill_is_idempotent(built):
    before = snapshot(built)
    run_backfill(built, START, END, chunk_days=5)
    run_backfill(built, date(2026, 9, 1), date(2026, 9, 2))
    assert snapshot(built) == before


def test_backdated_team_change_is_applied_by_backfill(events):
    engine = fresh_engine()
    load_events(engine, events)
    run_incremental(engine, now=FAR_FUTURE)
    # IdP correction: the contractor actually belonged to finance-bi all along
    engine.execute("INSERT INTO meta_ref.user_team_membership VALUES "
                   "('ext.contractor1', 'finance-bi', DATE '2026-01-01', NULL)")
    run_backfill(engine, START, END)
    assert rows(engine, "SELECT DISTINCT team_id FROM meta_dwh.fct_query "
                        "WHERE user_name = 'ext.contractor1'") == [("finance-bi",)]
    assert rows(engine, "SELECT COUNT(*) FROM meta_dwh.agg_team_daily "
                        "WHERE team_id = '__unassigned__'") == [(0,)]
