-- Q2. How many resources did team Y consume during a given period?
-- Params: team_id, start_date, end_date
--
-- Additive measures are summed from the daily team aggregate (via the cost
-- view); distinct users over the whole period come from fct_query (distinct
-- counts are not additive across days). Team = team of the user ON the day
-- of the query (point-in-time attribution).
WITH usage AS (
    SELECT
        team_id,
        SUM(query_count)           AS queries,
        SUM(failed_query_count)    AS failed_queries,
        SUM(cpu_hours)             AS cpu_hours,
        SUM(memory_gb_hours)       AS memory_gb_hours,
        SUM(scanned_tb)            AS scanned_tb,
        SUM(written_gb)            AS written_gb,
        SUM(spilled_gb)            AS spilled_gb,
        SUM(queued_hours)          AS queued_hours,
        SUM(failed_cpu_hours)      AS failed_cpu_hours,
        SUM(estimated_cost_usd)    AS estimated_cost_usd
    FROM meta_dwh.v_team_usage_daily
    WHERE team_id = '{{ team_id }}'
      AND usage_date BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}'
    GROUP BY team_id
),
people AS (
    SELECT team_id, COUNT(DISTINCT user_name) AS distinct_users
    FROM meta_dwh.fct_query
    WHERE team_id = '{{ team_id }}'
      AND query_date BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}'
    GROUP BY team_id
)
SELECT
    u.team_id,
    p.distinct_users,
    u.queries,
    u.failed_queries,
    ROUND(u.cpu_hours, 2)           AS cpu_hours,
    ROUND(u.memory_gb_hours, 2)     AS memory_gb_hours,
    ROUND(u.scanned_tb, 3)          AS scanned_tb,
    ROUND(u.written_gb, 2)          AS written_gb,
    ROUND(u.spilled_gb, 2)          AS spilled_gb,
    ROUND(u.queued_hours, 2)        AS queued_hours,
    ROUND(u.failed_cpu_hours, 2)    AS failed_cpu_hours,
    ROUND(u.estimated_cost_usd, 2)  AS estimated_cost_usd
FROM usage u
LEFT JOIN people p ON p.team_id = u.team_id;
