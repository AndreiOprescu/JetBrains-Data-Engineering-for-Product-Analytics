-- =============================================================================
-- RAW LAYER (source contract)
-- -----------------------------------------------------------------------------
-- In production this table is NOT created by the Meta DWH pipeline: it is the
-- landing table written by the Trino event listener sink (Trino -> Kafka ->
-- Iceberg, or the HTTP event listener -> collector -> Iceberg). It is included
-- here because it is the *contract* every transformation depends on, and so
-- the local demo can create it.
--
-- One row per QueryCompletedEvent emitted by the Trino coordinator. Column
-- names follow io.trino.spi.eventlistener.QueryCompletedEvent, flattened:
--   metadata.*   -> query_id, query_state, query_type, query_text, ...
--   context.*    -> user_name, principal, source, client_tags, resource_group_id
--   statistics.* -> *_time_ms, *_bytes, *_rows, cumulative_memory_*
--   ioMetadata   -> inputs (array of tables read), output_table (table written)
--   failureInfo  -> error_code, error_type, failure_message
--
-- Delivery is at-least-once, so the same query_id can appear more than once;
-- ingested_at is stamped by the sink and is used for de-duplication and for
-- incremental (watermark-based) processing.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS raw_trino;

CREATE TABLE IF NOT EXISTS raw_trino.query_completed (
    query_id                        VARCHAR,
    query_state                     VARCHAR,   -- FINISHED | FAILED
    query_type                      VARCHAR,   -- SELECT, INSERT, MERGE, DELETE, DATA_DEFINITION, EXPLAIN, DESCRIBE, ...
    update_type                     VARCHAR,   -- e.g. 'INSERT', 'CREATE TABLE' (NULL for reads)
    query_text                      VARCHAR,
    user_name                       VARCHAR,   -- context.user  (effective / impersonated session user)
    principal                       VARCHAR,   -- context.principal (authenticated identity)
    source                          VARCHAR,   -- client application, e.g. 'trino-jdbc', 'airflow', 'superset'
    client_tags                     ARRAY(VARCHAR),
    resource_group_id               VARCHAR,   -- e.g. 'global.etl.airflow'
    session_catalog                 VARCHAR,
    session_schema                  VARCHAR,
    environment                     VARCHAR,
    server_version                  VARCHAR,

    create_time                     TIMESTAMP(6) WITH TIME ZONE,
    execution_start_time            TIMESTAMP(6) WITH TIME ZONE,
    end_time                        TIMESTAMP(6) WITH TIME ZONE,

    queued_time_ms                  BIGINT,
    analysis_time_ms                BIGINT,
    planning_time_ms                BIGINT,
    wall_time_ms                    BIGINT,    -- execution wall time (excludes queueing)
    cpu_time_ms                     BIGINT,    -- total CPU time over all tasks/threads
    peak_user_memory_bytes          BIGINT,
    peak_total_memory_bytes         BIGINT,
    cumulative_memory_byte_seconds  DOUBLE,    -- integral of memory over time (see docs/assumptions.md)
    physical_input_bytes            BIGINT,    -- bytes read from storage (S3) by connectors
    physical_input_rows             BIGINT,
    processed_input_bytes           BIGINT,
    internal_network_bytes          BIGINT,    -- data shuffled between workers
    output_bytes                    BIGINT,    -- bytes returned to the client
    output_rows                     BIGINT,
    written_bytes                   BIGINT,    -- bytes written to tables
    written_rows                    BIGINT,
    spilled_bytes                   BIGINT,

    error_code                      VARCHAR,   -- e.g. 'EXCEEDED_TIME_LIMIT'
    error_type                      VARCHAR,   -- USER_ERROR | INTERNAL_ERROR | INSUFFICIENT_RESOURCES | EXTERNAL
    failure_message                 VARCHAR,

    -- ioMetadata.inputs: one element per table read (after view expansion).
    inputs ARRAY(ROW(
        catalog_name          VARCHAR,
        schema_name           VARCHAR,
        table_name            VARCHAR,
        columns               ARRAY(VARCHAR),
        physical_input_bytes  BIGINT,
        physical_input_rows   BIGINT
    )),
    -- ioMetadata.output: the table written by INSERT/CTAS/MERGE/..., NULL for reads.
    output_table ROW(
        catalog_name          VARCHAR,
        schema_name           VARCHAR,
        table_name            VARCHAR,
        columns               ARRAY(VARCHAR)
    ),

    ingested_at                     TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(end_time)']
);
