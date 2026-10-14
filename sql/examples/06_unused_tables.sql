-- Unused data lake tables: nobody has read them in the last N days.
-- Candidates for archiving / storage savings. Includes tables known from
-- ownership metadata that were never queried at all (first_access_date IS NULL).
-- Params: as_of_date, idle_days
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
