

SELECT
    week,
    earning_kind,
    avatars,
    avatars_in_app_tx
FROM `dbt`.`fct_execution_circles_v2_economically_active_avatars_weekly`
ORDER BY week DESC, earning_kind