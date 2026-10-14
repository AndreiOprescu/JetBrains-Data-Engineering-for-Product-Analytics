-- =============================================================================
-- META DWH (Iceberg tables in schema meta_dwh)
-- -----------------------------------------------------------------------------
-- Star schema:
--   dimensions : dim_date, dim_team, dim_user, dim_user_team_history, dim_table
--   facts      : fct_query          (1 row per query)
--                fct_table_access   (1 row per query x table x access type)
--   aggregates : agg_table_user_daily, agg_table_daily, agg_team_daily
--   ops        : etl_run_log
-- Natural keys are used as join keys (user_name, team_id, table_fqn): they are
-- stable, human-readable and avoid a key-lookup step in a lakehouse where
-- there is no enforced referential integrity anyway.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS meta_dwh;

-- -----------------------------------------------------------------------------
-- DIMENSIONS
-- -----------------------------------------------------------------------------

-- Calendar. Grain: one row per day.
CREATE TABLE IF NOT EXISTS meta_dwh.dim_date (
    date_day       DATE,
    year           INTEGER,
    quarter        INTEGER,
    month          INTEGER,
    iso_week       INTEGER,
    day_of_week    INTEGER,     -- 1 = Monday ... 7 = Sunday
    is_weekend     BOOLEAN,
    month_start    DATE,
    week_start     DATE         -- Monday of the ISO week
);

-- Teams. Grain: one row per team (SCD1) + the '__unassigned__' unknown member.
CREATE TABLE IF NOT EXISTS meta_dwh.dim_team (
    team_id        VARCHAR,
    team_name      VARCHAR,
    department     VARCHAR,
    cost_center    VARCHAR,
    is_active      BOOLEAN
);

-- User -> team history (SCD2, cleaned copy of meta_ref.user_team_membership).
-- Grain: one row per user per validity interval; intervals never overlap.
CREATE TABLE IF NOT EXISTS meta_dwh.dim_user_team_history (
    user_name      VARCHAR,
    team_id        VARCHAR,
    valid_from     DATE,
    valid_to       DATE,        -- exclusive; NULL = current
    is_current     BOOLEAN
);

-- Users / service accounts. Grain: one row per Trino user name (SCD1).
CREATE TABLE IF NOT EXISTS meta_dwh.dim_user (
    user_name        VARCHAR,
    account_type     VARCHAR,   -- human | service | unknown
    display_name     VARCHAR,
    current_team_id  VARCHAR,
    first_query_at   TIMESTAMP(6) WITH TIME ZONE,
    last_query_at    TIMESTAMP(6) WITH TIME ZONE,
    updated_at       TIMESTAMP(6) WITH TIME ZONE
);

-- Tables seen in query I/O metadata. Grain: one row per catalog.schema.table.
CREATE TABLE IF NOT EXISTS meta_dwh.dim_table (
    table_fqn          VARCHAR,  -- 'catalog.schema.table', lower-case
    catalog_name       VARCHAR,
    schema_name        VARCHAR,
    table_name         VARCHAR,
    connector_type     VARCHAR,
    is_data_lake       BOOLEAN,  -- Iceberg table on S3
    owner_team_id      VARCHAR,
    data_domain        VARCHAR,
    first_access_date  DATE,     -- NULL = known (catalog/ownership) but never queried
    last_access_date   DATE,
    updated_at         TIMESTAMP(6) WITH TIME ZONE
);

-- -----------------------------------------------------------------------------
-- FACTS
-- -----------------------------------------------------------------------------

-- Grain: one row per query (query_id), dated by completion day (UTC).
-- Every resource metric here is ADDITIVE across queries.
CREATE TABLE IF NOT EXISTS meta_dwh.fct_query (
    query_id                        VARCHAR,
    query_date                      DATE,
    create_time                     TIMESTAMP(6) WITH TIME ZONE,
    end_time                        TIMESTAMP(6) WITH TIME ZONE,
    user_name                       VARCHAR,
    principal                       VARCHAR,
    team_id                         VARCHAR,   -- team of user_name ON query_date (point-in-time)
    source                          VARCHAR,
    client_category                 VARCHAR,   -- scheduled | bi | interactive | other
    resource_group_id               VARCHAR,
    query_type                      VARCHAR,
    query_state                     VARCHAR,
    is_success                      BOOLEAN,
    error_type                      VARCHAR,
    error_code                      VARCHAR,
    input_table_count               INTEGER,
    output_table_fqn                VARCHAR,
    -- time
    queued_time_ms                  BIGINT,
    wall_time_ms                    BIGINT,
    elapsed_time_ms                 BIGINT,    -- end_time - create_time (what the user waited)
    cpu_time_ms                     BIGINT,
    -- memory
    peak_memory_bytes               BIGINT,
    cumulative_memory_byte_seconds  DOUBLE,
    -- I/O
    physical_input_bytes            BIGINT,
    physical_input_rows             BIGINT,
    processed_input_bytes           BIGINT,
    internal_network_bytes          BIGINT,
    output_bytes                    BIGINT,
    output_rows                     BIGINT,
    written_bytes                   BIGINT,
    written_rows                    BIGINT,
    spilled_bytes                   BIGINT,
    loaded_at                       TIMESTAMP(6) WITH TIME ZONE
)
WITH (partitioning = ARRAY['query_date']);

-- Grain: one row per (query_id, table_fqn, access_type).
-- access_type = 'READ' for every table in ioMetadata.inputs (deduplicated),
--               'WRITE' for ioMetadata.output.
-- allocation_weight splits the query's CPU / memory across the tables it
-- touched (sums to 1 per query), so allocated_* columns are additive across
-- tables; query_count-style measures are NOT (a query touching 3 tables
-- counts once for each of them).
CREATE TABLE IF NOT EXISTS meta_dwh.fct_table_access (
    query_id                                  VARCHAR,
    query_date                                DATE,
    table_fqn                                 VARCHAR,
    access_type                               VARCHAR,  -- READ | WRITE
    user_name                                 VARCHAR,
    team_id                                   VARCHAR,
    client_category                           VARCHAR,
    is_success                                BOOLEAN,
    is_metadata_table_only                    BOOLEAN,  -- only Iceberg metadata tables (t$snapshots, ...) were read
    columns_accessed                          ARRAY(VARCHAR),
    column_count                              INTEGER,
    physical_input_bytes                      BIGINT,   -- bytes read from THIS table (from Trino, not allocated)
    physical_input_rows                       BIGINT,
    written_bytes                             BIGINT,   -- WRITE rows only
    written_rows                              BIGINT,
    allocation_weight                         DOUBLE,
    allocated_cpu_time_ms                     DOUBLE,
    allocated_cumulative_memory_byte_seconds  DOUBLE,
    loaded_at                                 TIMESTAMP(6) WITH TIME ZONE
)
WITH (partitioning = ARRAY['query_date']);

-- -----------------------------------------------------------------------------
-- AGGREGATES (the historical / trend layer that dashboards read)
-- -----------------------------------------------------------------------------

-- Grain: day x table x user x access_type.
-- The key table for "distinct users of table X over ANY period": distinct
-- counts are not additive over days, but COUNT(DISTINCT user_name) over this
-- small table is exact and cheap for any date range.
CREATE TABLE IF NOT EXISTS meta_dwh.agg_table_user_daily (
    usage_date                                DATE,
    table_fqn                                 VARCHAR,
    user_name                                 VARCHAR,
    team_id                                   VARCHAR,
    access_type                               VARCHAR,
    query_count                               BIGINT,
    failed_query_count                        BIGINT,
    physical_input_bytes                      BIGINT,
    written_bytes                             BIGINT,
    allocated_cpu_time_ms                     DOUBLE,
    allocated_cumulative_memory_byte_seconds  DOUBLE
)
WITH (partitioning = ARRAY['month(usage_date)']);

-- Grain: day x table. Daily KPIs per table (daily distinct counts are
-- correct for the day only; do not sum them across days).
CREATE TABLE IF NOT EXISTS meta_dwh.agg_table_daily (
    usage_date                                DATE,
    table_fqn                                 VARCHAR,
    query_count                               BIGINT,   -- distinct queries touching the table
    read_query_count                          BIGINT,
    write_query_count                         BIGINT,
    failed_query_count                        BIGINT,
    distinct_users                            BIGINT,
    distinct_teams                            BIGINT,
    physical_input_bytes                      BIGINT,
    written_bytes                             BIGINT,
    allocated_cpu_time_ms                     DOUBLE,
    allocated_cumulative_memory_byte_seconds  DOUBLE
)
WITH (partitioning = ARRAY['month(usage_date)']);

-- Grain: day x team. Resource consumption per team (from fct_query, so
-- every query is counted exactly once, including queries touching no table).
CREATE TABLE IF NOT EXISTS meta_dwh.agg_team_daily (
    usage_date                      DATE,
    team_id                         VARCHAR,
    query_count                     BIGINT,
    failed_query_count              BIGINT,
    distinct_users                  BIGINT,
    distinct_tables_read            BIGINT,
    cpu_time_ms                     BIGINT,
    cumulative_memory_byte_seconds  DOUBLE,
    max_peak_memory_bytes           BIGINT,
    physical_input_bytes            BIGINT,
    written_bytes                   BIGINT,
    spilled_bytes                   BIGINT,
    queued_time_ms                  BIGINT,
    elapsed_time_ms                 BIGINT,
    failed_cpu_time_ms              BIGINT     -- CPU burnt by queries that failed
)
WITH (partitioning = ARRAY['month(usage_date)']);

-- -----------------------------------------------------------------------------
-- OPERATIONS
-- -----------------------------------------------------------------------------

-- One row per pipeline run; the last successful high_watermark drives the
-- next incremental run.
CREATE TABLE IF NOT EXISTS meta_dwh.etl_run_log (
    run_id           VARCHAR,
    started_at       TIMESTAMP(6) WITH TIME ZONE,
    finished_at      TIMESTAMP(6) WITH TIME ZONE,
    mode             VARCHAR,   -- incremental | backfill
    window_dates     VARCHAR,   -- processed dates, e.g. '2026-09-01..2026-09-03'
    high_watermark   TIMESTAMP(6) WITH TIME ZONE,  -- max raw ingested_at covered
    status           VARCHAR    -- SUCCESS | FAILED
);
