-- Data quality checks. Each statement returns the VIOLATING rows; an empty
-- result means the check passed. Run with:  python -m metadwh check
-- Params: start_date, end_date

-- check: fct_query has one row per query_id
SELECT query_id, COUNT(*) AS n
FROM meta_dwh.fct_query
WHERE query_date BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}'
GROUP BY query_id
HAVING COUNT(*) > 1;

-- check: every query with table access has allocation weights summing to 1
SELECT query_id, SUM(allocation_weight) AS total_weight
FROM meta_dwh.fct_table_access
WHERE query_date BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}'
GROUP BY query_id
HAVING abs(SUM(allocation_weight) - 1) > 1e-9;

-- check: allocated CPU reconciles with fct_query per day (within 1 ms)
SELECT f.query_date, f.cpu_ms, a.allocated_cpu_ms
FROM (
    SELECT query_date, SUM(cpu_time_ms) AS cpu_ms
    FROM meta_dwh.fct_query
    WHERE query_date BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}'
      AND query_id IN (SELECT query_id FROM meta_dwh.fct_table_access
                       WHERE query_date BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}')
    GROUP BY query_date
) f
JOIN (
    SELECT query_date, SUM(allocated_cpu_time_ms) AS allocated_cpu_ms
    FROM meta_dwh.fct_table_access
    WHERE query_date BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}'
    GROUP BY query_date
) a ON a.query_date = f.query_date
WHERE abs(f.cpu_ms - a.allocated_cpu_ms) > 1;

-- check: agg_team_daily reconciles with fct_query (query counts)
SELECT f.query_date, f.n, t.n
FROM (SELECT query_date, COUNT(*) AS n FROM meta_dwh.fct_query
      WHERE query_date BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}' GROUP BY query_date) f
LEFT JOIN (SELECT usage_date, SUM(query_count) AS n FROM meta_dwh.agg_team_daily
           WHERE usage_date BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}' GROUP BY usage_date) t
    ON t.usage_date = f.query_date
WHERE t.n IS NULL OR t.n <> f.n;

-- check: every fact team exists in dim_team
SELECT DISTINCT f.team_id
FROM meta_dwh.fct_query f
LEFT JOIN meta_dwh.dim_team t ON t.team_id = f.team_id
WHERE f.query_date BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}'
  AND t.team_id IS NULL;

-- check: every accessed table exists in dim_table
SELECT DISTINCT a.table_fqn
FROM meta_dwh.fct_table_access a
LEFT JOIN meta_dwh.dim_table d ON d.table_fqn = a.table_fqn
WHERE a.query_date BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}'
  AND d.table_fqn IS NULL;

-- warn: users with queries but no team mapping (fix in the IdP sync, then backfill)
SELECT user_name, COUNT(*) AS queries, MIN(query_date) AS first_day, MAX(query_date) AS last_day
FROM meta_dwh.fct_query
WHERE team_id = '__unassigned__'
  AND query_date BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}'
GROUP BY user_name;

-- warn: accessed data lake tables without an owner (fix in the data catalog)
SELECT table_fqn, last_access_date
FROM meta_dwh.dim_table
WHERE owner_team_id = '__unassigned__' AND is_data_lake AND last_access_date IS NOT NULL;
