-- agg_team_daily: day x team resource consumption, rebuilt for the window.
-- Built from fct_query so each query is counted exactly once (also queries
-- that touch no table, e.g. SHOW/EXPLAIN/SELECT 1, still consume resources).
DELETE FROM meta_dwh.agg_team_daily
WHERE usage_date BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}';

INSERT INTO meta_dwh.agg_team_daily
WITH q AS (
    SELECT
        query_date                                             AS usage_date,
        team_id,
        COUNT(*)                                               AS query_count,
        SUM(CASE WHEN is_success THEN 0 ELSE 1 END)            AS failed_query_count,
        COUNT(DISTINCT user_name)                              AS distinct_users,
        SUM(cpu_time_ms)                                       AS cpu_time_ms,
        SUM(cumulative_memory_byte_seconds)                    AS cumulative_memory_byte_seconds,
        MAX(peak_memory_bytes)                                 AS max_peak_memory_bytes,
        SUM(physical_input_bytes)                              AS physical_input_bytes,
        SUM(written_bytes)                                     AS written_bytes,
        SUM(spilled_bytes)                                     AS spilled_bytes,
        SUM(queued_time_ms)                                    AS queued_time_ms,
        SUM(elapsed_time_ms)                                   AS elapsed_time_ms,
        SUM(CASE WHEN is_success THEN 0 ELSE cpu_time_ms END)  AS failed_cpu_time_ms
    FROM meta_dwh.fct_query
    WHERE query_date BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}'
    GROUP BY query_date, team_id
),
t AS (
    SELECT
        query_date                 AS usage_date,
        team_id,
        COUNT(DISTINCT table_fqn)  AS distinct_tables_read
    FROM meta_dwh.fct_table_access
    WHERE query_date BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}'
      AND access_type = 'READ'
    GROUP BY query_date, team_id
)
SELECT
    q.usage_date,
    q.team_id,
    q.query_count,
    q.failed_query_count,
    q.distinct_users,
    COALESCE(t.distinct_tables_read, 0) AS distinct_tables_read,
    q.cpu_time_ms,
    q.cumulative_memory_byte_seconds,
    q.max_peak_memory_bytes,
    q.physical_input_bytes,
    q.written_bytes,
    q.spilled_bytes,
    q.queued_time_ms,
    q.elapsed_time_ms,
    q.failed_cpu_time_ms
FROM q
LEFT JOIN t
    ON t.usage_date = q.usage_date AND t.team_id = q.team_id;
