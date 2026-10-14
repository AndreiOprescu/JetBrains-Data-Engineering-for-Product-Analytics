"""Meta DWH pipeline: DDL, incremental runs, backfills.

Update model
------------
* Facts and aggregates are partitioned by day and rebuilt **per affected
  day** (DELETE partition + INSERT from raw). Rebuilding a day is idempotent,
  so retries, late events and backfills all use the same code path.
* *Which* days are affected is derived from the raw table's ``ingested_at``:
  every day that received at least one new event since the last successful
  run's high watermark is rebuilt. Late events (arriving days later) therefore
  automatically trigger a rebuild of the day they belong to.
* Reference-driven dimensions (teams, team history) are fully refreshed on
  every run; dim_user / dim_table are MERGEd.
* Changes to reference data that should rewrite history (e.g. a backdated
  team move) are applied with an explicit ``backfill`` of the affected range.
"""

from __future__ import annotations

import logging
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

from .engine import Engine, run_script

logger = logging.getLogger(__name__)

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"

DDL_FILES = ["ddl/02_reference.sql", "ddl/03_meta_dwh.sql", "ddl/04_seed_dim_date.sql"]
RAW_DDL_FILE = "ddl/01_raw.sql"
VIEW_FILES = ["views/01_cost_views.sql"]

# Steps that do not depend on the date window (run once per invocation).
GLOBAL_STEPS = ["transform/10_dim_team.sql", "transform/11_dim_user_team_history.sql"]
# Steps that rebuild the [start_date, end_date] window, in dependency order.
WINDOW_STEPS = [
    "transform/20_fct_query.sql",
    "transform/30_fct_table_access.sql",
    "transform/40_agg_table_user_daily.sql",
    "transform/41_agg_table_daily.sql",
    "transform/42_agg_team_daily.sql",
    "transform/50_dim_user.sql",
    "transform/51_dim_table.sql",
]


def _sql(rel: str) -> str:
    return (SQL_DIR / rel).read_text(encoding="utf-8")


def _ts_literal(ts: datetime) -> str:
    return f"TIMESTAMP '{ts.astimezone(timezone.utc):%Y-%m-%d %H:%M:%S.%f} UTC'"


def _as_utc(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def init(engine: Engine, with_raw: bool = False) -> None:
    """Create schemas, tables, calendar and views (idempotent)."""
    files = ([RAW_DDL_FILE] if with_raw else []) + DDL_FILES + VIEW_FILES
    for rel in files:
        logger.info("init: %s", rel)
        run_script(engine, _sql(rel))


def contiguous_ranges(days: Iterable[date]) -> list[tuple[date, date]]:
    """[d1, d2, d3, d7] -> [(d1, d3), (d7, d7)]"""
    ordered = sorted(set(days))
    ranges: list[tuple[date, date]] = []
    for d in ordered:
        if ranges and d == ranges[-1][1] + timedelta(days=1):
            ranges[-1] = (ranges[-1][0], d)
        else:
            ranges.append((d, d))
    return ranges


def run_window(engine: Engine, start: date, end: date) -> None:
    """Rebuild facts/aggregates for completion dates in [start, end]."""
    params = {"start_date": start.isoformat(), "end_date": end.isoformat()}
    for rel in WINDOW_STEPS:
        logger.info("window %s..%s: %s", start, end, rel)
        run_script(engine, _sql(rel), params)


def run_global_steps(engine: Engine) -> None:
    for rel in GLOBAL_STEPS:
        logger.info("global: %s", rel)
        run_script(engine, _sql(rel))


def _log_run(engine: Engine, run_id: str, started: datetime, mode: str, windows: str,
             high_watermark: datetime | None, status: str) -> None:
    wm = _ts_literal(high_watermark) if high_watermark else "NULL"
    windows = windows.replace("'", "")
    engine.execute(
        "INSERT INTO meta_dwh.etl_run_log "
        "(run_id, started_at, finished_at, mode, window_dates, high_watermark, status) VALUES "
        f"('{run_id}', {_ts_literal(started)}, current_timestamp, '{mode}', '{windows}', {wm}, '{status}')"
    )


def last_watermark(engine: Engine) -> datetime | None:
    rows = engine.execute(
        "SELECT max(high_watermark) FROM meta_dwh.etl_run_log WHERE status = 'SUCCESS'"
    )
    return _as_utc(rows[0][0]) if rows else None


def run_incremental(engine: Engine, safety_lag: timedelta = timedelta(minutes=15),
                    now: datetime | None = None) -> list[tuple[date, date]]:
    """Process every completion day that received new raw events since the
    last successful run. Returns the processed date ranges."""
    started = datetime.now(timezone.utc)
    now = now or started
    run_id = str(uuid.uuid4())
    previous = last_watermark(engine)

    # Upper bound: newest ingested event, but not newer than now - lag, to stay
    # clear of sink commits that may still be in flight with older timestamps.
    max_ingested = _as_utc(engine.execute("SELECT max(ingested_at) FROM raw_trino.query_completed")[0][0])
    if max_ingested is None:
        logger.info("raw table is empty, nothing to do")
        return []
    high = min(max_ingested, now - safety_lag)

    lower = f"ingested_at > {_ts_literal(previous)} AND " if previous else ""
    rows = engine.execute(
        "SELECT DISTINCT CAST(end_time AS DATE) FROM raw_trino.query_completed "
        f"WHERE {lower}ingested_at <= {_ts_literal(high)}"
    )
    days = [r[0] if isinstance(r[0], date) else date.fromisoformat(str(r[0])) for r in rows]
    ranges = contiguous_ranges(days)
    window_txt = ",".join(f"{a}..{b}" for a, b in ranges) or "none"
    logger.info("incremental run %s: watermark %s -> %s, windows %s", run_id, previous, high, window_txt)

    try:
        run_global_steps(engine)
        for start, end in ranges:
            run_window(engine, start, end)
    except Exception:
        _log_run(engine, run_id, started, "incremental", window_txt, previous, "FAILED")
        raise
    _log_run(engine, run_id, started, "incremental", window_txt, max(high, previous) if previous else high,
             "SUCCESS")
    return ranges


def run_backfill(engine: Engine, start: date, end: date, chunk_days: int = 7) -> None:
    """Rebuild an explicit date range (e.g. after a reference data correction)
    in chunks, without touching the incremental watermark."""
    started = datetime.now(timezone.utc)
    run_id = str(uuid.uuid4())
    run_global_steps(engine)
    cursor = start
    while cursor <= end:
        chunk_end = min(end, cursor + timedelta(days=chunk_days - 1))
        run_window(engine, cursor, chunk_end)
        cursor = chunk_end + timedelta(days=1)
    _log_run(engine, run_id, started, "backfill", f"{start}..{end}", None, "SUCCESS")
