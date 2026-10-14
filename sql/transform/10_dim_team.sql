-- dim_team: full refresh from meta_ref.team (small, SCD1) + unknown member.
-- Runs before the facts so every fct.team_id has a matching dimension row.
DELETE FROM meta_dwh.dim_team;

INSERT INTO meta_dwh.dim_team
SELECT
    lower(trim(team_id)) AS team_id,
    team_name,
    department,
    cost_center,
    COALESCE(is_active, TRUE) AS is_active
FROM meta_ref.team
UNION ALL
SELECT '__unassigned__', 'Unassigned (user not mapped to a team)', NULL, NULL, TRUE;
