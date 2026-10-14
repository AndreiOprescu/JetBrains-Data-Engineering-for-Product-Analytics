-- Most used / most expensive tables in a period.
-- Params: start_date, end_date
SELECT
    v.table_fqn,
    v.owner_team_id,
    SUM(v.query_count)                     AS queries,
    ROUND(SUM(v.scanned_gb), 1)            AS scanned_gb,
    ROUND(SUM(v.allocated_cpu_hours), 2)   AS allocated_cpu_hours,
    ROUND(SUM(v.estimated_cost_usd), 2)    AS estimated_cost_usd
FROM meta_dwh.v_table_usage_daily v
WHERE v.usage_date BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}'
GROUP BY v.table_fqn, v.owner_team_id
ORDER BY estimated_cost_usd DESC
LIMIT 10;
