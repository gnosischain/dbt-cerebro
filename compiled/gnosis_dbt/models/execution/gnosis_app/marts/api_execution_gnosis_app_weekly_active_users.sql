

-- Gnosis App Weekly Active Users (WAU) time-series — the in-app active population
-- (fct_execution_gnosis_app_users_weekly.active_users, same population/columns as DAU & MAU)
-- that WEAU (api_execution_gnosis_app_weekly_economically_active_users) is a strict subset of.
-- Latest incomplete week excluded.
SELECT
    week,
    active_users,
    new_users,
    returning_users,
    reactivated_users
FROM `dbt`.`fct_execution_gnosis_app_users_weekly`
WHERE week < toStartOfWeek(today(), 1)
ORDER BY week DESC