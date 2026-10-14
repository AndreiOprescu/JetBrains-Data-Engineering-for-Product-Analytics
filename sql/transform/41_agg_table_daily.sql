-- agg_table_daily: day x table KPIs, rebuilt for the window.
-- A query that reads and writes the same table (INSERT INTO t SELECT .. FROM t)
-- counts once in query_count, and once in each of read/write counts.
DELETE FROM meta_dwh.agg_table_daily
WHERE usage_date BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}';

INSERT INTO meta_dwh.agg_table_daily
SELECT
    query_date                                                         AS usage_date,
    table_fqn,
    COUNT(DISTINCT query_id)                                           AS query_count,
    COUNT(DISTINCT CASE WHEN access_type = 'READ'  THEN query_id END)  AS read_query_count,
    COUNT(DISTINCT CASE WHEN access_type = 'WRITE' THEN query_id END)  AS write_query_count,
    COUNT(DISTINCT CASE WHEN NOT is_success        THEN query_id END)  AS failed_query_count,
    COUNT(DISTINCT user_name)                                          AS distinct_users,
    COUNT(DISTINCT team_id)                                            AS distinct_teams,
    SUM(physical_input_bytes)                                          AS physical_input_bytes,
    SUM(written_bytes)                                                 AS written_bytes,
    SUM(allocated_cpu_time_ms)                                         AS allocated_cpu_time_ms,
    SUM(allocated_cumulative_memory_byte_seconds)                      AS allocated_cumulative_memory_byte_seconds
FROM meta_dwh.fct_table_access
WHERE query_date BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}'
GROUP BY query_date, table_fqn;
