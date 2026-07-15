

-- Gnosis App Monthly Active Users (MAU), "incl. Gnosis Pay" variant — monthly-grain member of the
-- active-users-incl-gpay triplet that powers the dashboard resolution toggle. Same New / Returning /
-- Reactivated / Active columns as the weekly variant; latest incomplete month excluded.
SELECT
    month,
    active_users,
    new_users,
    returning_users,
    reactivated_users
FROM `dbt`.`fct_execution_gnosis_app_users_monthly_incl_gpay`
WHERE month < toStartOfMonth(today())
ORDER BY month DESC