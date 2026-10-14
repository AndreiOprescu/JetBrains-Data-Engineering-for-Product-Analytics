-- agg_table_user_daily: day x table x user x access_type, rebuilt for the window.
DELETE FROM meta_dwh.agg_table_user_daily
WHERE usage_date BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}';

INSERT INTO meta_dwh.agg_table_user_daily
SELECT
    query_date                                       AS usage_date,
    table_fqn,
    user_name,
    team_id,
    access_type,
    COUNT(*)                                         AS query_count,
    SUM(CASE WHEN is_success THEN 0 ELSE 1 END)      AS failed_query_count,
    SUM(physical_input_bytes)                        AS physical_input_bytes,
    SUM(written_bytes)                               AS written_bytes,
    SUM(allocated_cpu_time_ms)                       AS allocated_cpu_time_ms,
    SUM(allocated_cumulative_memory_byte_seconds)    AS allocated_cumulative_memory_byte_seconds
FROM meta_dwh.fct_table_access
WHERE query_date BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}'
GROUP BY query_date, table_fqn, user_name, team_id, access_type;
