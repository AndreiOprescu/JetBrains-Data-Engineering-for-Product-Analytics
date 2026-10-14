-- Cost views: resource usage x versioned price list (meta_ref.cost_rate).
-- Cost is computed at read time, so a change to the price list never
-- requires reprocessing facts, and history keeps the rates valid at the time.

CREATE OR REPLACE VIEW meta_dwh.v_team_usage_daily AS
SELECT
    a.usage_date,
    a.team_id,
    t.team_name,
    t.department,
    t.cost_center,
    a.query_count,
    a.failed_query_count,
    a.distinct_users,
    a.distinct_tables_read,
    a.cpu_time_ms / 3600000e0                                          AS cpu_hours,
    a.cumulative_memory_byte_seconds / (1073741824e0 * 3600)           AS memory_gb_hours,
    a.physical_input_bytes / 1099511627776e0                           AS scanned_tb,
    a.written_bytes / 1073741824e0                                     AS written_gb,
    a.spilled_bytes / 1073741824e0                                     AS spilled_gb,
    a.queued_time_ms / 3600000e0                                       AS queued_hours,
    a.failed_cpu_time_ms / 3600000e0                                   AS failed_cpu_hours,
    a.cpu_time_ms / 3600000e0 * r.cpu_hour_usd
      + a.cumulative_memory_byte_seconds / (1073741824e0 * 3600) * r.memory_gb_hour_usd
      + a.physical_input_bytes / 1099511627776e0 * r.scanned_tb_usd    AS estimated_cost_usd
FROM meta_dwh.agg_team_daily a
LEFT JOIN meta_dwh.dim_team t
    ON t.team_id = a.team_id
LEFT JOIN meta_ref.cost_rate r
    ON a.usage_date >= r.valid_from AND (r.valid_to IS NULL OR a.usage_date < r.valid_to);

CREATE OR REPLACE VIEW meta_dwh.v_table_usage_daily AS
SELECT
    a.usage_date,
    a.table_fqn,
    d.is_data_lake,
    d.owner_team_id,
    d.data_domain,
    a.query_count,
    a.read_query_count,
    a.write_query_count,
    a.failed_query_count,
    a.distinct_users,
    a.distinct_teams,
    a.physical_input_bytes / 1073741824e0                              AS scanned_gb,
    a.written_bytes / 1073741824e0                                     AS written_gb,
    a.allocated_cpu_time_ms / 3600000e0                                AS allocated_cpu_hours,
    a.allocated_cpu_time_ms / 3600000e0 * r.cpu_hour_usd
      + a.allocated_cumulative_memory_byte_seconds / (1073741824e0 * 3600) * r.memory_gb_hour_usd
      + a.physical_input_bytes / 1099511627776e0 * r.scanned_tb_usd    AS estimated_cost_usd
FROM meta_dwh.agg_table_daily a
LEFT JOIN meta_dwh.dim_table d
    ON d.table_fqn = a.table_fqn
LEFT JOIN meta_ref.cost_rate r
    ON a.usage_date >= r.valid_from AND (r.valid_to IS NULL OR a.usage_date < r.valid_to);
