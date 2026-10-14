-- dim_user_team_history: cleaned SCD2 copy of meta_ref.user_team_membership.
-- Cleaning rules:
--   * user names are case-insensitive in practice -> lower(trim())
--   * a user has exactly ONE team at any point in time: if the source has
--     overlapping intervals, the interval that started later wins and the
--     earlier one is cut at that start date
--   * degenerate intervals (valid_from >= valid_to) are dropped
DELETE FROM meta_dwh.dim_user_team_history;

INSERT INTO meta_dwh.dim_user_team_history
WITH src AS (
    SELECT
        lower(trim(user_name)) AS user_name,
        lower(trim(team_id))   AS team_id,
        valid_from,
        valid_to
    FROM meta_ref.user_team_membership
    WHERE user_name IS NOT NULL AND team_id IS NOT NULL AND valid_from IS NOT NULL
),
cut AS (
    SELECT
        user_name,
        team_id,
        valid_from,
        CASE
            WHEN next_from IS NULL THEN valid_to
            WHEN valid_to IS NULL THEN next_from
            ELSE least(valid_to, next_from)
        END AS valid_to
    FROM (
        SELECT
            s.*,
            lead(valid_from) OVER (PARTITION BY user_name ORDER BY valid_from, team_id) AS next_from
        FROM src s
    ) x
)
SELECT
    user_name,
    team_id,
    valid_from,
    valid_to,
    valid_to IS NULL AS is_current
FROM cut
WHERE valid_to IS NULL OR valid_to > valid_from;
