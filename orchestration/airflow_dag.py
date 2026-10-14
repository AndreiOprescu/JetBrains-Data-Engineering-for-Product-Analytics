"""Airflow DAG for the Meta DWH (reference deployment).

* hourly   : sync reference data -> incremental run (rebuilds only the days that
             received new raw events since the last successful watermark)
             -> data quality checks for the last 3 days
* daily    : Iceberg maintenance (compaction + snapshot expiry)
* manual   : backfills are triggered with `python -m metadwh --engine trino
             backfill --start ... --end ...` (e.g. after a backdated team change)

The tasks shell out to the `metadwh` CLI so the same code path runs in
Airflow, in CI and on a laptop. Trino connection settings come from the
environment (TRINO_HOST, TRINO_USER, ...), e.g. an Airflow connection exported
as env vars by the worker.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}
CLI = "python -m metadwh --engine trino"

with DAG(
    dag_id="meta_dwh_hourly",
    start_date=datetime(2026, 1, 1),
    schedule="15 * * * *",
    catchup=False,             # the watermark, not the logical date, defines the work
    max_active_runs=1,         # runs must not overlap (watermark + partition rebuilds)
    default_args=DEFAULT_ARGS,
    tags=["meta-dwh"],
) as hourly:
    sync_reference = BashOperator(task_id="sync_reference", bash_command=f"{CLI} sync-reference")
    incremental = BashOperator(task_id="incremental_run", bash_command=f"{CLI} run --safety-lag-minutes 15")
    checks = BashOperator(
        task_id="data_quality_checks",
        bash_command=f"{CLI} check --start {{{{ macros.ds_add(ds, -3) }}}} --end {{{{ ds }}}}",
    )
    sync_reference >> incremental >> checks

with DAG(
    dag_id="meta_dwh_maintenance",
    start_date=datetime(2026, 1, 1),
    schedule="30 3 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["meta-dwh"],
) as maintenance:
    BashOperator(
        task_id="iceberg_maintenance",
        bash_command=f"{CLI} query sql/maintenance/iceberg_maintenance.sql -p since_date={{{{ macros.ds_add(ds, -7) }}}}",
    )
