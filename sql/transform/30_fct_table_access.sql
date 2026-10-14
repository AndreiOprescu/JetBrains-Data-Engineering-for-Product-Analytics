-- fct_table_access: one row per (query, table, access type) for the run window.
--
-- Multi-table queries (joins, UNIONs, cross-catalog federation, INSERT ...
-- SELECT) produce one row per table. Each row gets:
--   * the bytes/rows Trino reports for THAT table (exact, not allocated)
--   * an allocation_weight used to split the query's CPU and memory:
--       READ rows : share of the query's physical input bytes read from the
--                   table (equal split if no table reports bytes)
--       WRITE row : 0 when the query also read tables (the read side carries
--                   the cost), 1 for pure writes (INSERT ... VALUES)
--     -> weights sum to 1 per query, so allocated_* is additive across tables
--        and reconciles with fct_query.
-- The same table listed several times (self-join) is collapsed to one row;
-- Iceberg metadata tables (orders$snapshots, orders$files) are attributed to
-- their base table and flagged.
-- Depends on fct_query (user/team/state come from there: one attribution rule).
DELETE FROM meta_dwh.fct_table_access
WHERE query_date BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}';

INSERT INTO meta_dwh.fct_table_access
WITH raw_window AS (
    SELECT
        r.query_id,
        r.inputs,
        r.output_table,
        row_number() OVER (PARTITION BY r.query_id ORDER BY r.ingested_at DESC) AS rn
    FROM raw_trino.query_completed r
    WHERE CAST(r.end_time AS DATE) BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}'
      AND r.query_id IS NOT NULL
),
q AS (
    SELECT query_id, inputs, output_table FROM raw_window WHERE rn = 1
),
read_items AS (
    SELECT
        q.query_id,
        lower(i.catalog_name)    AS catalog_name,
        lower(i.schema_name)     AS schema_name,
        lower(i.table_name)      AS raw_table_name,
        i.columns                AS columns,
        i.physical_input_bytes   AS physical_input_bytes,
        i.physical_input_rows    AS physical_input_rows
    FROM q
    CROSS JOIN UNNEST(q.inputs) AS i(catalog_name, schema_name, table_name, columns,
                                     physical_input_bytes, physical_input_rows)
    WHERE i.table_name IS NOT NULL
),
reads AS (
    SELECT
        query_id,
        catalog_name || '.' || schema_name || '.' || split_part(raw_table_name, '$', 1) AS table_fqn,
        bool_and(strpos(raw_table_name, '$') > 0)                            AS is_metadata_table_only,
        array_sort(array_distinct(flatten(
            array_agg(columns) FILTER (WHERE columns IS NOT NULL))))          AS columns_accessed,
        SUM(COALESCE(physical_input_bytes, 0))                               AS physical_input_bytes,
        SUM(COALESCE(physical_input_rows, 0))                                AS physical_input_rows
    FROM read_items
    GROUP BY 1, 2
),
read_totals AS (
    SELECT query_id, SUM(physical_input_bytes) AS total_bytes, COUNT(*) AS n_tables
    FROM reads
    GROUP BY query_id
),
writes AS (
    SELECT
        q.query_id,
        lower(q.output_table.catalog_name) || '.' || lower(q.output_table.schema_name)
            || '.' || lower(q.output_table.table_name) AS table_fqn,
        q.output_table.columns                          AS columns_accessed
    FROM q
    WHERE q.output_table.table_name IS NOT NULL
),
access AS (
    SELECT
        r.query_id,
        r.table_fqn,
        'READ'                       AS access_type,
        r.is_metadata_table_only,
        r.columns_accessed,
        r.physical_input_bytes,
        r.physical_input_rows,
        CASE
            WHEN t.total_bytes > 0 THEN CAST(r.physical_input_bytes AS DOUBLE) / t.total_bytes
            ELSE CAST(1 AS DOUBLE) / t.n_tables
        END                          AS allocation_weight
    FROM reads r
    JOIN read_totals t ON t.query_id = r.query_id
    UNION ALL
    SELECT
        w.query_id,
        w.table_fqn,
        'WRITE'                      AS access_type,
        FALSE                        AS is_metadata_table_only,
        w.columns_accessed,
        CAST(0 AS BIGINT)            AS physical_input_bytes,
        CAST(0 AS BIGINT)            AS physical_input_rows,
        CASE WHEN t.query_id IS NULL THEN CAST(1 AS DOUBLE) ELSE CAST(0 AS DOUBLE) END AS allocation_weight
    FROM writes w
    LEFT JOIN read_totals t ON t.query_id = w.query_id
)
SELECT
    a.query_id,
    f.query_date,
    a.table_fqn,
    a.access_type,
    f.user_name,
    f.team_id,
    f.client_category,
    f.is_success,
    a.is_metadata_table_only,
    a.columns_accessed,
    CAST(COALESCE(cardinality(a.columns_accessed), 0) AS INTEGER)              AS column_count,
    a.physical_input_bytes,
    a.physical_input_rows,
    CASE WHEN a.access_type = 'WRITE' THEN f.written_bytes ELSE 0 END          AS written_bytes,
    CASE WHEN a.access_type = 'WRITE' THEN f.written_rows  ELSE 0 END          AS written_rows,
    a.allocation_weight,
    f.cpu_time_ms * a.allocation_weight                                        AS allocated_cpu_time_ms,
    f.cumulative_memory_byte_seconds * a.allocation_weight                     AS allocated_cumulative_memory_byte_seconds,
    current_timestamp                                                          AS loaded_at
FROM access a
JOIN meta_dwh.fct_query f
    ON  f.query_id = a.query_id
    AND f.query_date BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}';
