# Design decisions

## 1. Source: Trino's `QueryCompletedEvent`

Trino calls registered event listeners once per query with a
`QueryCompletedEvent`. That one event carries everything this model needs:

* **who**: `context.user` (the effective user, after impersonation), `principal`, `source` (client app), resource group
* **what**: `ioMetadata.inputs` (every table read, with columns and per-table physical bytes/rows) and `ioMetadata.output` (table written)
* **how much**: `statistics` (CPU, wall and queued time, peak and cumulative memory, physical input, written and spilled bytes)
* **how it ended**: `queryState` and `failureInfo`

`system.runtime.queries` is not an option for history: it only keeps recent
queries held in coordinator memory. The raw table schema used here
(`sql/ddl/01_raw.sql`) is a flattened copy of the event and is treated as a
**contract**. If the actual sink names things differently, only the two fact
transformations need to change.

## 2. A star schema with two facts at different grains

The two questions need different grains:

* *Resources per team* is a property of a **query**. Counting it once per table
  would double-count a join.
* *Users per table* is a property of a **query × table** pair.

Squeezing both into one table forces one of them to be wrong or awkward. So
there are two facts: `fct_query` (one row per query) and `fct_table_access`
(one row per query × table × access type). They share `query_id`, and user and
team come from a single place (`fct_query`), so there's exactly one attribution
rule. Everything else is a conformed dimension (`dim_team`, `dim_user`,
`dim_table`, `dim_date`).

On top of the facts, three small **daily aggregates** form the historical
layer that dashboards and the example queries read. The facts stay the detailed
source for drill-down and for rebuilding aggregates.

## 3. Mapping users to teams: point-in-time, with "current" also available

People change teams. If a user moves from Product Analytics to ML on
1 September, their August queries should still count for Product Analytics, or
budgets and trends get rewritten after the fact. So:

* `meta_ref.user_team_membership` holds the history (`valid_from`, `valid_to`).
* `dim_user_team_history` is a cleaned SCD2 version of it. User names are
  lower-cased, and overlapping intervals are cut so a user has exactly one team
  on any given day: the most recently started interval wins.
* The team is **resolved at load time** and stored on the fact
  (`fct_query.team_id`). It's the team on the query's day.
* For "current org chart" reporting, join `dim_user.current_team_id` instead.
* Users with no mapping go to `__unassigned__`. They are never dropped, and a
  data-quality warning lists them.
* Service accounts are mapped to the team that owns them. With Superset-style
  impersonation, `user` is the real end user and `principal` is the service
  account, and we attribute to `user`.

Tested in `test_point_in_time_team_attribution` and
`test_backdated_team_change_is_applied_by_backfill`.

## 4. Multi-table queries

Trino lists every table a query read (`inputs`, after view expansion) and the
table it wrote (`output`). The rules are:

| Situation | Rule |
|---|---|
| Join / UNION of N tables | N `READ` rows. **Usage counts:** the query counts once for *each* table (a user who ran a join did use both tables). |
| Resource attribution | `allocation_weight` = the table's share of the query's physical input bytes; **equal split** if no table reports bytes. Weights sum to 1 per query, so `allocated_cpu_time_ms` and `allocated_cumulative_memory_byte_seconds` add up exactly to `fct_query` (checked by a reconciliation check). |
| Per-table bytes | Not allocated: Trino reports `physical_input_bytes` per input, and that exact number is stored. |
| Cross-catalog (Iceberg + PostgreSQL) | Same rules. JDBC connectors report rows but 0 bytes, so in a mixed query the federated table gets weight 0. This is a documented bias: the Iceberg/S3 side is what this project measures, and an alternative weighting by rows is a one-line change. |
| Self-join (same table twice) | Collapsed into one row: bytes summed, columns unioned. |
| Iceberg metadata tables (`t$snapshots`, `t$files`) | Attributed to the base table `t`, with `is_metadata_table_only` set so they can be filtered out. |
| `INSERT … SELECT` / CTAS | `READ` rows carry the cost; the `WRITE` row gets weight 0, plus `written_bytes`/`written_rows`. |
| `INSERT … VALUES` (no input) | The `WRITE` row gets weight 1. |
| Read and write the same table | One `READ` and one `WRITE` row. `agg_table_daily.query_count` counts the query once. |
| No table at all (`SELECT 1`, `SHOW`, analysis failures) | Only in `fct_query`. Still counted in team resource usage. |

Alternatives considered for the weights:
* **Full attribution** (every table gets 100% of the query's CPU) is simple,
  but sums no longer reconcile, so "cost per table" is overstated.
* **Equal split** ignores that a 2 TB fact table and a 10 KB lookup table are
  very different.
* **Bytes share** (chosen) uses data Trino already measures. Scan volume is the
  main driver of CPU in a data-lake workload.

## 5. Resource usage metrics

| Metric (column) | Meaning | Why it's in the model |
|---|---|---|
| `cpu_time_ms` | CPU time summed over all tasks and threads | **Primary compute metric.** It doesn't depend on cluster load or parallelism, so it compares fairly across teams and over time. Wall time doesn't: 1 s on 64 cores ≠ 1 s on 1 core. |
| `cumulative_memory_byte_seconds` | Memory integrated over time | Memory reserved × how long it was held is what blocks other queries on a shared cluster. Peak alone ignores duration. |
| `peak_memory_bytes` | Peak total memory | Capacity planning and memory-limit tuning (`max_peak_memory_bytes` per team and day). |
| `physical_input_bytes` (+ rows) | Bytes read from S3 | Drives S3 GET and egress cost and I/O. It's also the clearest signal of "unpartitioned full scans" to optimise. |
| `written_bytes` (+ rows) | Bytes written to tables | Storage growth from ETL and sandboxes. |
| `spilled_bytes` | Spill to disk | Sign of queries that are too big for memory. |
| `queued_time_ms`, `elapsed_time_ms`, `wall_time_ms` | Waiting, end-to-end and execution time | User experience and cluster contention, *not* cost. |
| `failed_cpu_time_ms` | CPU used by failed queries | Waste. Failed queries are kept in all metrics, because they used resources. |
| `estimated_cost_usd` (view) | CPU-h × rate + GB-h memory × rate + TB scanned × rate | One number for budgets. Computed **at read time** from a *versioned* price list (`meta_ref.cost_rate`), so changing prices never requires reprocessing, and history keeps the rates that were valid then. |

Units are kept raw in the tables (ms, bytes, byte-seconds) so nothing is lost
to rounding. The views convert them to hours, GB and TB.

## 6. Distinct users over arbitrary periods

`distinct_users` can't be added up across days: a user active on 5 days isn't
5 users. Three ways to get exact distinct counts:

1. Scan `fct_table_access` for the period. Correct, but this is the largest
   table.
2. **`agg_table_user_daily` (chosen)**: one row per day × table × user. It's
   small, and `COUNT(DISTINCT user_name)` over any date range is exact.
3. HyperLogLog sketches (Trino `approx_set` stored as `varbinary`, then
   `merge` + `cardinality`). Approximate, but fixed size. This is the upgrade
   path for millions of users. With internal Trino users (hundreds to
   thousands) it isn't needed.

Daily `distinct_users` in `agg_table_daily` / `agg_team_daily` are still
useful for daily charts and are labelled "daily only".

## 7. How the data is updated

**Incremental, partition-rebuild, idempotent.**

1. Each run reads the last successful `high_watermark` from `etl_run_log`
   (the max raw `ingested_at` already processed).
2. It finds every **completion day** that received at least one new event
   since then: `SELECT DISTINCT CAST(end_time AS DATE) … WHERE ingested_at > watermark`.
   Iceberg file-level min/max statistics on `ingested_at` keep this scan cheap,
   because new events are in new files.
3. For each contiguous range of affected days, every fact and aggregate
   partition in the range is **deleted and rebuilt from raw**. On
   identity-partitioned Iceberg tables, the DELETE is a metadata-only
   operation.
4. Reference dimensions (`dim_team`, `dim_user_team_history`) are fully
   refreshed. `dim_user` and `dim_table` are MERGEd: activity ranges widen,
   and attributes are refreshed for all rows.
5. The new watermark is stored. It's capped at `now − safety lag` (15 min) so
   that sink commits still in flight are picked up next time.

Properties that fall out of this design (all tested):

* **Duplicates** (at-least-once delivery): de-duplicated per `query_id`, keeping the last delivered copy.
* **Late events**: an event that arrives days later reopens *its* day, which is rebuilt in full (`test_incremental_batches_with_late_events_equal_full_rebuild`: loading in 4 batches gives exactly the same tables as one full load).
* **Retries and re-runs** are safe (`test_backfill_is_idempotent`).
* **Reference corrections** (for example, a backdated team change): run
  `backfill --start … --end …` over the affected range. It uses the same code
  path in weekly chunks.
* **Logic changes**: deploy, then backfill. Raw is the replayable source of
  truth.

**Schedule.** The incremental run is hourly (`orchestration/airflow_dag.py`).
Normally it only rebuilds today and sometimes yesterday, so its cost is about
one day of raw data. Data-quality checks run after each run. A daily
maintenance job compacts recent partitions and expires snapshots
(`sql/maintenance/iceberg_maintenance.sql`). Delete + insert creates small
files and many snapshots, so this matters.

**Known trade-off.** DELETE and INSERT are two Iceberg commits. A reader can,
for a few seconds, see a day with no rows. If that matters, write to an
Iceberg branch and publish with a fast-forward (write-audit-publish), or
replace the pair with a single `MERGE`. The trade-off was accepted here for
simplicity.

**Retention.** Raw events (which include `query_text`, which may contain
literals and PII) have short retention (e.g. 90 days), and the text is **not**
copied into the Meta DWH. Facts keep 13 months. Aggregates and dimensions are
kept indefinitely: they are small and they *are* the history.

## 8. Other decisions

* **UTC days** everywhere (`query_date` = UTC date of `end_time`). A query is
  dated by when it *completed*, which is when its event exists.
* **The pipeline measures itself.** It connects with `source = 'meta-dwh-pipeline'`
  and the client tag `meta_dwh`, so its own consumption shows up in the model
  under its service account.
* **One SQL dialect, two engines.** Trino SQL is the single source of truth.
  DuckDB + sqlglot run the same files locally and in CI, with no cluster
  needed. The only dialect gaps are handled in `DuckDBEngine.translate`.
* **Plain SQL files plus a small Python runner** rather than a framework. The
  steps map one to one to dbt models (`delete+insert` incremental strategy
  with `unique_key = partition`), if the team already uses dbt.

## 9. Possible extensions

* Column-level usage: `columns_accessed` is already stored per access. An
  `agg_column_daily` would show which columns are never read.
* View-level usage: newer Trino versions emit `metadata.tables` with
  `referenceChain` / `directlyReferenced`. That could add a `VIA_VIEW` access
  type.
* Storage-side metrics: snapshot table size and file counts from the Iceberg
  `$files`/`$snapshots` metadata into a `fct_table_storage_daily`, so cost per
  table covers storage as well as compute.
* Alerting on anomalies, for example a team's weekly CPU growing more than 50%
  (see example query 05).
