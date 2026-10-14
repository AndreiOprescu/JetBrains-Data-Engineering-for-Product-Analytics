-- Iceberg table maintenance for the Meta DWH (Trino only; run daily).
-- The hourly DELETE + INSERT of the current day's partitions creates small
-- files and many snapshots; compact the recent partitions and expire old
-- snapshots so reads stay fast and S3 storage does not grow unbounded.
-- Params: since_date (e.g. 7 days ago)

ALTER TABLE meta_dwh.fct_query        EXECUTE optimize WHERE query_date >= DATE '{{ since_date }}';
ALTER TABLE meta_dwh.fct_table_access EXECUTE optimize WHERE query_date >= DATE '{{ since_date }}';
ALTER TABLE meta_dwh.agg_table_user_daily EXECUTE optimize WHERE usage_date >= DATE '{{ since_date }}';
ALTER TABLE meta_dwh.agg_table_daily  EXECUTE optimize WHERE usage_date >= DATE '{{ since_date }}';
ALTER TABLE meta_dwh.agg_team_daily   EXECUTE optimize WHERE usage_date >= DATE '{{ since_date }}';

ALTER TABLE meta_dwh.fct_query            EXECUTE expire_snapshots(retention_threshold => '7d');
ALTER TABLE meta_dwh.fct_table_access     EXECUTE expire_snapshots(retention_threshold => '7d');
ALTER TABLE meta_dwh.agg_table_user_daily EXECUTE expire_snapshots(retention_threshold => '7d');
ALTER TABLE meta_dwh.agg_table_daily      EXECUTE expire_snapshots(retention_threshold => '7d');
ALTER TABLE meta_dwh.agg_team_daily       EXECUTE expire_snapshots(retention_threshold => '7d');
ALTER TABLE meta_dwh.dim_user             EXECUTE expire_snapshots(retention_threshold => '7d');
ALTER TABLE meta_dwh.dim_table            EXECUTE expire_snapshots(retention_threshold => '7d');

ALTER TABLE meta_dwh.fct_query        EXECUTE remove_orphan_files(retention_threshold => '7d');
ALTER TABLE meta_dwh.fct_table_access EXECUTE remove_orphan_files(retention_threshold => '7d');
