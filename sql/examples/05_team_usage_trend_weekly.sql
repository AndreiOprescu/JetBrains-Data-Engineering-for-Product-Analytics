-- History: weekly resource consumption per team with week-over-week change.
-- Params: start_date, end_date
WITH weekly AS (
    SELECT
        c.week_start,
        v.team_id,
        SUM(v.query_count)         AS queries,
        SUM(v.cpu_hours)           AS cpu_hours,
        SUM(v.scanned_tb)          AS scanned_tb,
        SUM(v.estimated_cost_usd)  AS estimated_cost_usd
    FROM meta_dwh.v_team_usage_daily v
    JOIN meta_dwh.dim_date c
        ON c.date_day = v.usage_date
    WHERE v.usage_date BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}'
    GROUP BY c.week_start, v.team_id
)
SELECT
    week_start,
    team_id,
    queries,
    ROUND(cpu_hours, 1)            AS cpu_hours,
    ROUND(scanned_tb, 2)           AS scanned_tb,
    ROUND(estimated_cost_usd, 2)   AS estimated_cost_usd,
    ROUND(100.0 * (cpu_hours / NULLIF(lag(cpu_hours) OVER (PARTITION BY team_id ORDER BY week_start), 0) - 1), 1)
                                   AS cpu_change_pct
FROM weekly
ORDER BY team_id, week_start;
