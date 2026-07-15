

-- Gnosis App Weekly Active Users (WAU), "incl. Gnosis Pay" variant — resolution-suffixed twin of
-- api_execution_gnosis_app_weekly_active_users_incl_gpay, part of the daily/weekly/monthly triplet
-- that powers the dashboard resolution toggle. Same population and columns; latest incomplete week
-- excluded.
SELECT
    week,
    active_users,
    new_users,
    returning_users,
    reactivated_users
FROM `dbt`.`fct_execution_gnosis_app_users_weekly_incl_gpay`
WHERE week < toStartOfWeek(today(), 1)
ORDER BY week DESC