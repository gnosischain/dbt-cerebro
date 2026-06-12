

SELECT sub.*, (SELECT toDate(max(date)) FROM `dbt`.`int_execution_gnosis_app_user_activity_daily`) AS as_of_date
FROM (
SELECT
    anyIf(retention_pct, months_since = 1
                     AND cohort_month = (
                       SELECT max(cohort_month)
                       FROM `dbt`.`fct_execution_gnosis_app_retention_monthly`
                       WHERE months_since = 1
                     )
    )                                                  AS value,
    CAST(NULL AS Nullable(Float64))                    AS change_pct
FROM `dbt`.`fct_execution_gnosis_app_retention_monthly`
) AS sub