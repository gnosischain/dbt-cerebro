

SELECT
    max(n_ga_wallets_cumulative)                      AS value,
    CAST(NULL AS Nullable(Float64))                   AS change_pct
FROM `dbt`.`fct_execution_gnosis_app_gpay_wallets_daily`
WHERE onboarding_class = 'imported'
  AND date < today()