

SELECT
    week,
    is_blacklisted,
    cnt
FROM `dbt`.`fct_execution_gnosis_app_weekly_active_users`
WHERE week < toStartOfWeek(today(), 1)
ORDER BY week DESC, is_blacklisted