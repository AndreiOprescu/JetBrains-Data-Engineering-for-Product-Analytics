# Example queries

All example queries are in `sql/examples/` and take `{{ name }}` parameters. Run them with

```bash
python -m metadwh [--engine trino] query sql/examples/<file>.sql -p name=value ...
```

The outputs below come from the local demo (`python -m metadwh demo`): 61 days of synthetic events
from 24 accounts (19 people, 5 service accounts) in 6 teams, 2026-08-01 to 2026-09-30. Note that in this data, `emil.novak` moves from
Product Analytics to ML on 2026-09-01, and the ML team's usage grows over time.

## Q1. How many distinct users queried table X during a given period?

File: `sql/examples/01_distinct_users_of_table.sql`. Parameters: `table_fqn=lake.events.ide_usage`, `start_date=2026-09-01`, `end_date=2026-09-30`

```sql
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
```

Result:

```text
table_fqn              distinct_users  distinct_readers  distinct_writers  distinct_human_users  distinct_service_accounts  distinct_teams  query_table_accesses
---------------------  --------------  ----------------  ----------------  --------------------  -------------------------  --------------  --------------------
lake.events.ide_usage               9                 9                 0                     8                          1               2                  1782
```

## Q2. How many resources did team Y consume during a given period?

File: `sql/examples/02_team_resource_usage.sql`. Parameters: `team_id=ml-recsys`, `start_date=2026-09-01`, `end_date=2026-09-30`

```sql
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
```

Result:

```text
team_id    distinct_users  queries  failed_queries  cpu_hours  memory_gb_hours  scanned_tb  written_gb  spilled_gb  queued_hours  failed_cpu_hours  estimated_cost_usd
---------  --------------  -------  --------------  ---------  ---------------  ----------  ----------  ----------  ------------  ----------------  ------------------
ml-recsys               5     3736             252     566.10           230.53       71.06    1,395.55      330.16         20.72             10.38              130.27
```

## Q2b. Where did team Y's resources go? Breakdown by table for a period.

File: `sql/examples/03_team_resources_by_table.sql`. Parameters: `team_id=ml-recsys`, `start_date=2026-09-01`, `end_date=2026-09-30`

```sql
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
```

Result:

```text
table_fqn                owner_team_id      query_table_accesses  distinct_users  allocated_cpu_hours  allocated_memory_gb_hours  scanned_gb  pct_of_team_cpu
-----------------------  -----------------  --------------------  --------------  -------------------  -------------------------  ----------  ---------------
lake.events.ide_usage    product-analytics                  1038               5               369.50                     196.14   47,780.90            65.30
lake.ml.training_set_v3  ml-recsys                          1055               5                88.94                      18.51   11,541.80            15.70
lake.ml.user_features    ml-recsys                          1490               5                67.66                      11.08    8,331.70            12.00
lake.ml.recommendations  ml-recsys                           963               5                32.07                       3.87    4,079.60             5.70
lake.product.users       product-analytics                   510               4                 7.92                       0.93    1,029.20             1.40
lake.sandbox.anna_tmp    __unassigned__                      155               4                 0.00                       0.00        0.00             0.00
```

## History: weekly distinct users and queries of table X.

File: `sql/examples/04_table_usage_trend_weekly.sql`. Parameters: `table_fqn=lake.events.ide_usage`, `start_date=2026-08-03`, `end_date=2026-09-27`

```sql
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
```

Result:

```text
week_start  distinct_users  distinct_teams  query_table_accesses  scanned_gb
----------  --------------  --------------  --------------------  ----------
2026-08-03               9               2                   338   15,790.40
2026-08-10               9               2                   394   17,301.70
2026-08-17               9               2                   362   15,348.00
2026-08-24               9               2                   418   19,161.40
2026-08-31               9               2                   429   18,538.20
2026-09-07               9               2                   427   18,345.00
2026-09-14               9               2                   403   18,105.00
2026-09-21               8               2                   377   17,265.70
```

## History: weekly resource consumption per team with week-over-week change.

File: `sql/examples/05_team_usage_trend_weekly.sql`. Parameters: `start_date=2026-08-03`, `end_date=2026-09-27`

```sql
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
```

Result (filtered to two teams for brevity):

```text
week_start  team_id            queries  cpu_hours  scanned_tb  estimated_cost_usd  cpu_change_pct
----------  -----------------  -------  ---------  ----------  ------------------  --------------
2026-08-03  ml-recsys              382      63.30        7.89               14.84  NULL          
2026-08-10  ml-recsys              456      73.70        9.07               17.13           16.40
2026-08-17  ml-recsys              513      79.80        9.71               18.32            8.30
2026-08-24  ml-recsys              530      92.30       10.79               20.61           15.60
2026-08-31  ml-recsys              777     118.20       14.19               26.30           28.10
2026-09-07  ml-recsys              786     114.30       14.95               27.20           -3.30
2026-09-14  ml-recsys              876     136.90       17.20               31.57           19.80
2026-09-21  ml-recsys              891     135.60       16.87               30.99           -1.00
2026-08-03  product-analytics      603      98.50       11.87               22.43  NULL          
2026-08-10  product-analytics      639     101.50       12.96               24.22            3.00
2026-08-17  product-analytics      599      82.90       10.61               19.82          -18.30
2026-08-24  product-analytics      657     106.40       13.57               25.39           28.30
2026-08-31  product-analytics      476      79.80       10.50               19.22          -25.00
2026-09-07  product-analytics      487      76.00        9.84               17.91           -4.80
2026-09-14  product-analytics      406      63.20        7.49               13.97          -16.90
2026-09-21  product-analytics      339      56.70        6.80               12.56          -10.30
```

## Unused data lake tables: nobody has read them in the last N days.

File: `sql/examples/06_unused_tables.sql`. Parameters: `as_of_date=2026-09-30`, `idle_days=30`

```sql
-- Candidates for archiving / storage savings. Includes tables known from
-- ownership metadata that were never queried at all (first_access_date IS NULL).
SELECT
    d.table_fqn,
    d.owner_team_id,
    d.data_domain,
    d.first_access_date,
    d.last_access_date,
    date_diff('day', d.last_access_date, DATE '{{ as_of_date }}') AS days_since_last_access
FROM meta_dwh.dim_table d
WHERE d.is_data_lake
  AND (d.last_access_date IS NULL
       OR d.last_access_date < date_add('day', -{{ idle_days }}, DATE '{{ as_of_date }}'))
ORDER BY d.last_access_date NULLS FIRST, d.table_fqn;
```

Result:

```text
table_fqn                owner_team_id      data_domain  first_access_date  last_access_date  days_since_last_access
-----------------------  -----------------  -----------  -----------------  ----------------  ----------------------
lake.legacy.events_2019  data-platform      telemetry    NULL               NULL              NULL                  
lake.legacy.licenses_v1  product-analytics  product      NULL               NULL              NULL                  
```

## Who consumes whose data? Owner team x consumer team, for a period.

File: `sql/examples/07_cross_team_consumption.sql`. Parameters: `start_date=2026-09-01`, `end_date=2026-09-30`

```sql
-- Useful for chargeback discussions and for finding critical shared tables.
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
```

Result:

```text
owner_team           consumer_team        tables_used  distinct_users  query_table_accesses  allocated_cpu_hours
-------------------  -------------------  -----------  --------------  --------------------  -------------------
__unassigned__       data-platform                  2               3                   297                 0.33
data-platform        data-platform                  4               4                  2193               217.95
data-platform        marketing-analytics            1               4                   441                74.87
data-platform        product-analytics              1               4                   365                29.66
data-platform        __unassigned__                 1               1                   318                29.13
finance-bi           finance-bi                     2               3                   983                 4.83
marketing-analytics  marketing-analytics            2               4                  1744                40.92
ml-recsys            ml-recsys                      3               5                  3418               188.67
product-analytics    ml-recsys                      2               5                  1548               377.42
product-analytics    product-analytics              4               4                  2013               263.97
product-analytics    sales-ops                      2               3                   811                 6.78
product-analytics    marketing-analytics            1               4                   348                 5.33
product-analytics    __unassigned__                 1               1                   285                 4.59
product-analytics    finance-bi                     2               3                   513                 3.71
sales-ops            sales-ops                      2               3                  1009                 0.02
```

## Most used / most expensive tables in a period.

File: `sql/examples/08_top_tables.sql`. Parameters: `start_date=2026-09-01`, `end_date=2026-09-30`

```sql
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
```

Result:

```text
table_fqn                   owner_team_id        queries  scanned_gb  allocated_cpu_hours  estimated_cost_usd
--------------------------  -------------------  -------  ----------  -------------------  ------------------
lake.events.ide_usage       product-analytics       1782   79,542.00               618.78              142.83
lake.events.page_views      data-platform           1370   26,864.00               206.04               47.82
lake.events.sessions        data-platform           1308   11,971.10                96.55               21.47
lake.ml.training_set_v3     ml-recsys               1055   11,541.80                88.94               20.55
lake.ml.user_features       ml-recsys               1490    8,331.70                67.66               14.96
lake.marketing.attribution  marketing-analytics      901    5,041.30                40.45                9.02
lake.events.clicks          data-platform            594    4,841.40                37.49                8.62
lake.ml.recommendations     ml-recsys                963    4,079.60                32.07                7.28
lake.product.users          product-analytics       1707    3,451.00                26.76                6.14
lake.product.licenses       product-analytics       1260    1,431.20                11.30                2.55
```
