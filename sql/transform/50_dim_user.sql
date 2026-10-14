-- dim_user (SCD1). Two idempotent MERGEs:
--   1) register users active in the window and widen their activity range
--   2) refresh descriptive attributes for ALL users (team changes, renames,
--      account type), because they can change without any new query
MERGE INTO meta_dwh.dim_user t
USING (
    SELECT
        user_name,
        MIN(create_time) AS first_query_at,
        MAX(end_time)    AS last_query_at
    FROM meta_dwh.fct_query
    WHERE query_date BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}'
    GROUP BY user_name
) s
ON t.user_name = s.user_name
WHEN MATCHED THEN UPDATE SET
    first_query_at = least(t.first_query_at, s.first_query_at),
    last_query_at  = greatest(t.last_query_at, s.last_query_at),
    updated_at     = current_timestamp
WHEN NOT MATCHED THEN INSERT
    (user_name, account_type, display_name, current_team_id, first_query_at, last_query_at, updated_at)
VALUES
    (s.user_name, 'unknown', NULL, '__unassigned__', s.first_query_at, s.last_query_at, current_timestamp);

MERGE INTO meta_dwh.dim_user t
USING (
    SELECT
        u.user_name,
        COALESCE(a.account_type,
                 CASE WHEN u.user_name LIKE 'svc%' THEN 'service' ELSE 'unknown' END) AS account_type,
        a.display_name,
        COALESCE(h.team_id, '__unassigned__')                                       AS current_team_id
    FROM meta_dwh.dim_user u
    LEFT JOIN (
        SELECT lower(trim(user_name)) AS user_name,
               MAX(account_type)      AS account_type,
               MAX(display_name)      AS display_name
        FROM meta_ref.user_account
        GROUP BY 1
    ) a ON a.user_name = u.user_name
    LEFT JOIN meta_dwh.dim_user_team_history h
        ON h.user_name = u.user_name AND h.is_current
) s
ON t.user_name = s.user_name
WHEN MATCHED THEN UPDATE SET
    account_type    = s.account_type,
    display_name    = s.display_name,
    current_team_id = s.current_team_id;
