

SELECT
    count(*)   AS value,
    CAST(NULL AS Nullable(Float64)) AS change_pct
FROM `dbt`.`int_execution_gnosis_app_users_current`