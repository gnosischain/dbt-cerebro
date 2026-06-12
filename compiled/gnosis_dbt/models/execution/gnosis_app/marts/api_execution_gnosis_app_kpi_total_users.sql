

SELECT sub.*, (SELECT toDate(max(block_timestamp)) FROM `dbt`.`int_execution_gnosis_app_user_events`) AS as_of_date
FROM (
SELECT
    count(*)   AS value,
    CAST(NULL AS Nullable(Float64)) AS change_pct
FROM `dbt`.`int_execution_gnosis_app_users_current`
) AS sub