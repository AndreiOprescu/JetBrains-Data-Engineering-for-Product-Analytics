-- Who consumes whose data? Owner team x consumer team, for a period.
-- Useful for chargeback discussions and for finding critical shared tables.
-- Params: start_date, end_date
SELECT
    d.owner_team_id                                         AS owner_team,
    a.team_id                                               AS consumer_team,
    COUNT(DISTINCT a.table_fqn)                             AS tables_used,
    COUNT(DISTINCT a.user_name)                             AS distinct_users,
    SUM(a.query_count)                                      AS query_table_accesses,
    ROUND(SUM(a.allocated_cpu_time_ms) / 3600000e0, 2)      AS allocated_cpu_hours
FROM meta_dwh.agg_table_user_daily a
JOIN meta_dwh.dim_table d
    ON d.table_fqn = a.table_fqn
WHERE a.access_type = 'READ'
  AND a.usage_date BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}'
GROUP BY d.owner_team_id, a.team_id
ORDER BY owner_team, allocated_cpu_hours DESC;
