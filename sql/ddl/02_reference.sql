-- =============================================================================
-- REFERENCE LAYER (additional metadata that Trino events do not contain)
-- -----------------------------------------------------------------------------
-- These tables are synced from systems of record (IdP / HR system, data
-- catalog, FinOps config). They are small, fully replaced on every sync
-- (see metadwh/reference_sync.py) and never written by the transformations.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS meta_ref;

-- Teams (source: HR system / IdP groups). One row per team.
CREATE TABLE IF NOT EXISTS meta_ref.team (
    team_id        VARCHAR,     -- stable identifier, e.g. 'data-platform'
    team_name      VARCHAR,
    department     VARCHAR,
    cost_center    VARCHAR,
    is_active      BOOLEAN
);

-- Trino login -> team history (source: IdP group membership, e.g. Okta/LDAP).
-- Validity interval is half-open: [valid_from, valid_to); valid_to NULL = current.
-- Service accounts are listed here as well, mapped to the team that owns them.
CREATE TABLE IF NOT EXISTS meta_ref.user_team_membership (
    user_name      VARCHAR,     -- Trino user name (context.user), case-insensitive
    team_id        VARCHAR,
    valid_from     DATE,
    valid_to       DATE
);

-- Account catalogue (source: IdP). Lets us split human vs service usage.
CREATE TABLE IF NOT EXISTS meta_ref.user_account (
    user_name      VARCHAR,
    account_type   VARCHAR,     -- 'human' | 'service'
    display_name   VARCHAR,
    is_active      BOOLEAN
);

-- Table ownership (source: data catalog / repo CODEOWNERS).
-- table_name = '*' assigns a default owner to a whole schema; an exact table
-- entry always wins over the schema default.
CREATE TABLE IF NOT EXISTS meta_ref.table_ownership (
    catalog_name   VARCHAR,
    schema_name    VARCHAR,
    table_name     VARCHAR,
    owner_team_id  VARCHAR,
    data_domain    VARCHAR
);

-- Trino catalogs (source: Trino catalog properties). Tells Iceberg/S3 data
-- apart from federated sources (PostgreSQL, ...) and Trino internals.
CREATE TABLE IF NOT EXISTS meta_ref.catalog (
    catalog_name   VARCHAR,
    connector_type VARCHAR,     -- 'iceberg' | 'postgresql' | 'system' | ...
    is_data_lake   BOOLEAN      -- TRUE for the Iceberg tables on S3 we want to track
);

-- Internal price list used to turn resource usage into an (estimated) cost.
-- Versioned so that historical cost is computed with the rates valid then.
CREATE TABLE IF NOT EXISTS meta_ref.cost_rate (
    valid_from          DATE,
    valid_to            DATE,   -- exclusive, NULL = current
    cpu_hour_usd        DOUBLE, -- per CPU-hour of Trino worker time
    memory_gb_hour_usd  DOUBLE, -- per GB-hour of cumulative memory
    scanned_tb_usd      DOUBLE  -- per TB read from S3 (GET requests + egress proxy)
);
