

WITH

events AS (
    SELECT
        solver,
        event_name,
        block_timestamp,
        row_number() OVER (PARTITION BY solver ORDER BY block_timestamp DESC, log_index DESC) AS rn
    FROM `dbt`.`stg_cow__solvers`
),

latest_status AS (
    SELECT
        solver,
        event_name,
        block_timestamp AS last_changed_at
    FROM events
    WHERE rn = 1
)

SELECT
    solver,
    event_name = 'SolverAdded'                                                       AS is_active,
    last_changed_at
FROM latest_status