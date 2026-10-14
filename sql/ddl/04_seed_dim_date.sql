-- Calendar seed (idempotent): 2024-01-01 .. 2030-12-31.
DELETE FROM meta_dwh.dim_date;

INSERT INTO meta_dwh.dim_date
SELECT
    d                                          AS date_day,
    year(d)                                    AS year,
    quarter(d)                                 AS quarter,
    month(d)                                   AS month,
    week(d)                                    AS iso_week,
    day_of_week(d)                             AS day_of_week,
    day_of_week(d) IN (6, 7)                   AS is_weekend,
    CAST(date_trunc('month', d) AS DATE)       AS month_start,
    CAST(date_trunc('week', d) AS DATE)        AS week_start
FROM UNNEST(sequence(DATE '2024-01-01', DATE '2030-12-31', INTERVAL '1' DAY)) AS t(d);
