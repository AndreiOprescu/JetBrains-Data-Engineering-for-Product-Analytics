-- fct_query: one row per query, for the completion dates in the run window.
-- Idempotent: the window's partitions are deleted and fully rebuilt from raw,
-- so re-running a window (late events, bug fixes, mapping corrections) is safe.
DELETE FROM meta_dwh.fct_query
WHERE query_date BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}';

INSERT INTO meta_dwh.fct_query
WITH raw_window AS (
    SELECT
        r.*,
        -- at-least-once delivery: keep the last delivered copy of each query
        row_number() OVER (PARTITION BY r.query_id ORDER BY r.ingested_at DESC) AS rn
    FROM raw_trino.query_completed r
    WHERE CAST(r.end_time AS DATE) BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}'
      AND r.query_id IS NOT NULL
),
q AS (
    SELECT
        rw.*,
        lower(trim(rw.user_name))  AS user_key,
        CAST(rw.end_time AS DATE)  AS query_day
    FROM raw_window rw
    WHERE rw.rn = 1
)
SELECT
    q.query_id,
    q.query_day                                         AS query_date,
    q.create_time,
    q.end_time,
    q.user_key                                          AS user_name,
    lower(trim(q.principal))                            AS principal,
    -- point-in-time attribution: the team the user belonged to on that day
    COALESCE(h.team_id, '__unassigned__')               AS team_id,
    q.source,
    CASE
        WHEN lower(q.source) LIKE 'airflow%' OR lower(q.source) LIKE 'dbt%'
          OR lower(q.source) LIKE 'spark%'   OR lower(q.source) LIKE 'scheduler%'   THEN 'scheduled'
        WHEN lower(q.source) LIKE 'superset%' OR lower(q.source) LIKE 'tableau%'
          OR lower(q.source) LIKE 'looker%'   OR lower(q.source) LIKE 'metabase%'   THEN 'bi'
        WHEN lower(q.source) LIKE 'trino-cli%' OR lower(q.source) LIKE 'trino-jdbc%'
          OR lower(q.source) LIKE 'datagrip%'  OR lower(q.source) LIKE 'jupyter%'
          OR lower(q.source) LIKE 'trino-python%'                                   THEN 'interactive'
        ELSE 'other'
    END                                                 AS client_category,
    q.resource_group_id,
    q.query_type,
    q.query_state,
    q.query_state = 'FINISHED'                          AS is_success,
    q.error_type,
    q.error_code,
    CAST(COALESCE(cardinality(q.inputs), 0) AS INTEGER) AS input_table_count,
    CASE WHEN q.output_table.table_name IS NOT NULL THEN
        lower(q.output_table.catalog_name) || '.' || lower(q.output_table.schema_name)
            || '.' || lower(q.output_table.table_name)
    END                                                 AS output_table_fqn,
    COALESCE(q.queued_time_ms, 0)                       AS queued_time_ms,
    COALESCE(q.wall_time_ms, 0)                         AS wall_time_ms,
    date_diff('millisecond', q.create_time, q.end_time) AS elapsed_time_ms,
    COALESCE(q.cpu_time_ms, 0)                          AS cpu_time_ms,
    COALESCE(q.peak_total_memory_bytes, q.peak_user_memory_bytes, 0) AS peak_memory_bytes,
    COALESCE(q.cumulative_memory_byte_seconds, 0)       AS cumulative_memory_byte_seconds,
    COALESCE(q.physical_input_bytes, 0)                 AS physical_input_bytes,
    COALESCE(q.physical_input_rows, 0)                  AS physical_input_rows,
    COALESCE(q.processed_input_bytes, 0)                AS processed_input_bytes,
    COALESCE(q.internal_network_bytes, 0)               AS internal_network_bytes,
    COALESCE(q.output_bytes, 0)                         AS output_bytes,
    COALESCE(q.output_rows, 0)                          AS output_rows,
    COALESCE(q.written_bytes, 0)                        AS written_bytes,
    COALESCE(q.written_rows, 0)                         AS written_rows,
    COALESCE(q.spilled_bytes, 0)                        AS spilled_bytes,
    current_timestamp                                   AS loaded_at
FROM q
LEFT JOIN meta_dwh.dim_user_team_history h
    ON  h.user_name = q.user_key
    AND q.query_day >= h.valid_from
    AND (h.valid_to IS NULL OR q.query_day < h.valid_to);
