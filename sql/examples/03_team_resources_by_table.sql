-- Q2b. Where did team Y's resources go? Breakdown by table for a period.
-- Params: team_id, start_date, end_date
--
-- allocated_* columns split each query's CPU/memory over the tables it
-- touched, so the rows add up to the team's table-related total. Queries that
-- touched no table (SHOW, EXPLAIN, failed parsing) are not in this breakdown.
SELECT
    a.table_fqn,
    d.owner_team_id,
    SUM(a.query_count)                                              AS query_table_accesses,
    COUNT(DISTINCT a.user_name)                                     AS distinct_users,
    ROUND(SUM(a.allocated_cpu_time_ms) / 3600000e0, 2)              AS allocated_cpu_hours,
    ROUND(SUM(a.allocated_cumulative_memory_byte_seconds)
          / (1073741824e0 * 3600), 2)                               AS allocated_memory_gb_hours,
    ROUND(SUM(a.physical_input_bytes) / 1073741824e0, 1)            AS scanned_gb,
    ROUND(100.0 * SUM(a.allocated_cpu_time_ms)
          / SUM(SUM(a.allocated_cpu_time_ms)) OVER (), 1)           AS pct_of_team_cpu
FROM meta_dwh.agg_table_user_daily a
LEFT JOIN meta_dwh.dim_table d
    ON d.table_fqn = a.table_fqn
WHERE a.team_id = '{{ team_id }}'
  AND a.usage_date BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}'
GROUP BY a.table_fqn, d.owner_team_id
ORDER BY allocated_cpu_hours DESC;
