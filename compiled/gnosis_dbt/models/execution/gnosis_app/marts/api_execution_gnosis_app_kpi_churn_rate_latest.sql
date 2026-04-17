

SELECT
    anyIf(churn_rate, month = (
        SELECT max(month)
        FROM `dbt`.`fct_execution_gnosis_app_churn_monthly`
        WHERE scope = 'Any'
    ) AND scope = 'Any')                               AS value,
    CAST(NULL AS Nullable(Float64))                    AS change_pct
FROM `dbt`.`fct_execution_gnosis_app_churn_monthly`