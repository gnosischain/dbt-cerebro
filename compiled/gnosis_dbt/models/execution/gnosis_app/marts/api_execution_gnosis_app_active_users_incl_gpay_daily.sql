

-- Gnosis App Daily Active Users (DAU), "incl. Gnosis Pay" variant — daily-grain member of the
-- active-users-incl-gpay triplet that powers the dashboard resolution toggle. Same New / Returning /
-- Reactivated / Active columns as the weekly variant; latest incomplete day (today) excluded.
SELECT
    date,
    active_users,
    new_users,
    returning_users,
    reactivated_users
FROM `dbt`.`fct_execution_gnosis_app_users_daily_incl_gpay`
WHERE date < today()
ORDER BY date DESC