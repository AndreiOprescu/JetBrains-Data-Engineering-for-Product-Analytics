-- History: weekly distinct users and queries of table X.
-- Params: table_fqn, start_date, end_date
SELECT
    c.week_start,
    COUNT(DISTINCT a.user_name)                         AS distinct_users,
    COUNT(DISTINCT a.team_id)                           AS distinct_teams,
    SUM(a.query_count)                                  AS query_table_accesses,
    ROUND(SUM(a.physical_input_bytes) / 1073741824e0, 1) AS scanned_gb
FROM meta_dwh.agg_table_user_daily a
JOIN meta_dwh.dim_date c
    ON c.date_day = a.usage_date
WHERE a.table_fqn = '{{ table_fqn }}'
  AND a.usage_date BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}'
GROUP BY c.week_start
ORDER BY c.week_start;
