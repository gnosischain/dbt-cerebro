

SELECT
    toString(activity_month) AS x,
    toString(cohort_month)   AS y,
    retention_pct            AS retention_pct,
    users                    AS value_abs,
    amount_retention_pct     AS amount_retention_pct,
    amount_usd               AS value_usd
FROM `dbt`.`fct_execution_gnosis_app_token_offer_claims_cohort_monthly`
ORDER BY y, x