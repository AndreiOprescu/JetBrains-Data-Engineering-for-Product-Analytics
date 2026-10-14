-- dim_table. Three idempotent steps:
--   1) register tables accessed in the window and widen their access range
--   2) register tables that are known from ownership metadata but have never
--      been queried (so "unused table" reports can see them)
--   3) refresh descriptive attributes for ALL tables (owner, domain, connector)
-- Assumption: catalog/schema/table identifiers contain no dots, so table_fqn
-- can be split with split_part().
MERGE INTO meta_dwh.dim_table t
USING (
    SELECT
        table_fqn,
        MIN(usage_date) AS first_access_date,
        MAX(usage_date) AS last_access_date
    FROM meta_dwh.agg_table_daily
    WHERE usage_date BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}'
    GROUP BY table_fqn
) s
ON t.table_fqn = s.table_fqn
WHEN MATCHED THEN UPDATE SET
    first_access_date = least(COALESCE(t.first_access_date, s.first_access_date), s.first_access_date),
    last_access_date  = greatest(COALESCE(t.last_access_date, s.last_access_date), s.last_access_date),
    updated_at        = current_timestamp
WHEN NOT MATCHED THEN INSERT
    (table_fqn, catalog_name, schema_name, table_name, connector_type, is_data_lake,
     owner_team_id, data_domain, first_access_date, last_access_date, updated_at)
VALUES
    (s.table_fqn, split_part(s.table_fqn, '.', 1), split_part(s.table_fqn, '.', 2),
     split_part(s.table_fqn, '.', 3), NULL, NULL, NULL, NULL,
     s.first_access_date, s.last_access_date, current_timestamp);

MERGE INTO meta_dwh.dim_table t
USING (
    SELECT DISTINCT
        lower(catalog_name) || '.' || lower(schema_name) || '.' || lower(table_name) AS table_fqn,
        lower(catalog_name) AS catalog_name,
        lower(schema_name)  AS schema_name,
        lower(table_name)   AS table_name
    FROM meta_ref.table_ownership
    WHERE table_name <> '*'
) s
ON t.table_fqn = s.table_fqn
WHEN NOT MATCHED THEN INSERT
    (table_fqn, catalog_name, schema_name, table_name, connector_type, is_data_lake,
     owner_team_id, data_domain, first_access_date, last_access_date, updated_at)
VALUES
    (s.table_fqn, s.catalog_name, s.schema_name, s.table_name, NULL, NULL, NULL, NULL,
     NULL, NULL, current_timestamp);

MERGE INTO meta_dwh.dim_table t
USING (
    SELECT
        d.table_fqn,
        c.connector_type,
        COALESCE(c.is_data_lake, FALSE)          AS is_data_lake,
        COALESCE(o.owner_team_id, '__unassigned__') AS owner_team_id,
        o.data_domain
    FROM meta_dwh.dim_table d
    LEFT JOIN (
        SELECT lower(catalog_name) AS catalog_name,
               MAX(connector_type) AS connector_type,
               bool_or(is_data_lake) AS is_data_lake
        FROM meta_ref.catalog
        GROUP BY 1
    ) c ON c.catalog_name = d.catalog_name
    LEFT JOIN (
        -- exact table entry beats the schema-level default ('*')
        SELECT table_fqn, owner_team_id, data_domain
        FROM (
            SELECT
                d2.table_fqn,
                lower(o2.owner_team_id) AS owner_team_id,
                o2.data_domain,
                row_number() OVER (
                    PARTITION BY d2.table_fqn
                    ORDER BY CASE WHEN o2.table_name = '*' THEN 1 ELSE 0 END, o2.owner_team_id
                ) AS rk
            FROM meta_dwh.dim_table d2
            JOIN meta_ref.table_ownership o2
                ON  lower(o2.catalog_name) = d2.catalog_name
                AND lower(o2.schema_name)  = d2.schema_name
                AND (lower(o2.table_name)  = d2.table_name OR o2.table_name = '*')
        ) ranked
        WHERE rk = 1
    ) o ON o.table_fqn = d.table_fqn
) s
ON t.table_fqn = s.table_fqn
WHEN MATCHED THEN UPDATE SET
    connector_type = s.connector_type,
    is_data_lake   = s.is_data_lake,
    owner_team_id  = s.owner_team_id,
    data_domain    = s.data_domain;
