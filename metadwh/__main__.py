"""Command line entry point:  python -m metadwh <command> [options]

  init            create schemas/tables/views (add --with-raw for local raw table)
  sync-reference  load reference_data/*.csv into meta_ref
  generate        append synthetic Trino events to raw_trino.query_completed
  run             incremental run (watermark-driven)
  backfill        rebuild an explicit date range
  query           run an example query file with parameters
  check           run data quality checks
  demo            local end-to-end demo on DuckDB (init + data + run + examples)
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

from .engine import format_rows, make_engine, render, split_statements
from .pipeline import SQL_DIR, init, run_backfill, run_incremental
from .reference_sync import sync_reference_data
from .sample_data import generate_events, load_events

ROOT = Path(__file__).resolve().parent.parent


def _params(pairs: list[str]) -> dict[str, str]:
    out = {}
    for pair in pairs or []:
        key, _, value = pair.partition("=")
        out[key.strip()] = value.strip()
    return out


def run_query_file(engine, path: Path, params: dict[str, str], max_rows: int = 50) -> None:
    for statement in split_statements(render(path.read_text(encoding="utf-8"), params)):
        cols, rows = engine.query(statement)
        print(format_rows(cols, rows, max_rows))
        print()


def run_checks(engine, params: dict[str, str]) -> int:
    failures = 0
    for statement in split_statements(render((SQL_DIR / "checks/data_quality.sql").read_text(), params)):
        title = next((l.strip("- ").strip() for l in statement.splitlines()
                      if l.strip().startswith("-- check:") or l.strip().startswith("-- warn:")), "check")
        _, rows = engine.query(statement)
        is_warning = title.startswith("warn:")
        status = "OK  " if not rows else ("WARN" if is_warning else "FAIL")
        if rows and not is_warning:
            failures += 1
        print(f"[{status}] {title} ({len(rows)} rows)")
    return failures


def demo(engine) -> None:
    start, days = date(2026, 8, 1), 61
    end = start + timedelta(days=days - 1)
    init(engine, with_raw=True)
    sync_reference_data(engine, ROOT / "reference_data")
    events = generate_events(start, days)
    load_events(engine, events)
    print(f"Loaded {len(events):,} raw events for {start}..{end}")
    ranges = run_incremental(engine)
    print(f"Processed date ranges: {ranges}\n")

    examples = [
        ("01_distinct_users_of_table.sql", {"table_fqn": "lake.events.ide_usage",
                                            "start_date": "2026-09-01", "end_date": "2026-09-30"}),
        ("02_team_resource_usage.sql", {"team_id": "ml-recsys",
                                        "start_date": "2026-09-01", "end_date": "2026-09-30"}),
        ("03_team_resources_by_table.sql", {"team_id": "ml-recsys",
                                            "start_date": "2026-09-01", "end_date": "2026-09-30"}),
        ("04_table_usage_trend_weekly.sql", {"table_fqn": "lake.events.ide_usage",
                                             "start_date": str(start), "end_date": str(end)}),
        ("05_team_usage_trend_weekly.sql", {"start_date": "2026-08-03", "end_date": "2026-09-27"}),
        ("06_unused_tables.sql", {"as_of_date": str(end), "idle_days": "30"}),
        ("07_cross_team_consumption.sql", {"start_date": "2026-09-01", "end_date": "2026-09-30"}),
        ("08_top_tables.sql", {"start_date": "2026-09-01", "end_date": "2026-09-30"}),
    ]
    for name, params in examples:
        print(f"== {name}  {params}")
        run_query_file(engine, SQL_DIR / "examples" / name, params, max_rows=15)
    print("== data quality checks")
    run_checks(engine, {"start_date": str(start), "end_date": str(end)})


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="metadwh", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--engine", choices=["duckdb", "trino"], default="duckdb")
    parser.add_argument("--duckdb-path", default="meta_dwh.duckdb")
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("init")
    p.add_argument("--with-raw", action="store_true", help="also create the raw landing table")
    p = sub.add_parser("sync-reference")
    p.add_argument("--dir", default=str(ROOT / "reference_data"))
    p = sub.add_parser("generate")
    p.add_argument("--start", type=date.fromisoformat, default=date(2026, 8, 1))
    p.add_argument("--days", type=int, default=61)
    p.add_argument("--seed", type=int, default=42)
    p = sub.add_parser("run")
    p.add_argument("--safety-lag-minutes", type=int, default=15)
    p = sub.add_parser("backfill")
    p.add_argument("--start", type=date.fromisoformat, required=True)
    p.add_argument("--end", type=date.fromisoformat, required=True)
    p = sub.add_parser("query")
    p.add_argument("file")
    p.add_argument("--param", "-p", action="append", help="name=value")
    p = sub.add_parser("check")
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    sub.add_parser("demo")

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING,
                        format="%(asctime)s %(levelname)s %(message)s")
    engine = make_engine(args.engine, args.duckdb_path)
    try:
        if args.command == "init":
            init(engine, with_raw=args.with_raw)
        elif args.command == "sync-reference":
            print(sync_reference_data(engine, args.dir))
        elif args.command == "generate":
            print(f"loaded {load_events(engine, generate_events(args.start, args.days, args.seed)):,} events")
        elif args.command == "run":
            print(run_incremental(engine, safety_lag=timedelta(minutes=args.safety_lag_minutes)))
        elif args.command == "backfill":
            run_backfill(engine, args.start, args.end)
        elif args.command == "query":
            run_query_file(engine, Path(args.file), _params(args.param))
        elif args.command == "check":
            return 1 if run_checks(engine, {"start_date": args.start, "end_date": args.end}) else 0
        elif args.command == "demo":
            demo(engine)
    finally:
        engine.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
