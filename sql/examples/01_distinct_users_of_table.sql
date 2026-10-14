-- Q1. How many distinct users queried table X during a given period?
-- Params: table_fqn, start_date, end_date
--
-- Reads the day x table x user aggregate: COUNT(DISTINCT) over it is exact for
-- ANY period (daily distinct counts can't simply be summed across days).
SELECT
    a.table_fqn,
    COUNT(DISTINCT a.user_name)                                                     AS distinct_users,
    COUNT(DISTINCT CASE WHEN a.access_type = 'READ'  THEN a.user_name END)          AS distinct_readers,
    COUNT(DISTINCT CASE WHEN a.access_type = 'WRITE' THEN a.user_name END)          AS distinct_writers,
    COUNT(DISTINCT CASE WHEN u.account_type = 'human' THEN a.user_name END)         AS distinct_human_users,
    COUNT(DISTINCT CASE WHEN u.account_type = 'service' THEN a.user_name END)       AS distinct_service_accounts,
    COUNT(DISTINCT a.team_id)                                                       AS distinct_teams,
    SUM(a.query_count)                                                              AS query_table_accesses
FROM meta_dwh.agg_table_user_daily a
LEFT JOIN meta_dwh.dim_user u
    ON u.user_name = a.user_name
WHERE a.table_fqn = '{{ table_fqn }}'
  AND a.usage_date BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}'
GROUP BY a.table_fqn;
