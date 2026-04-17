

SELECT
    date,
    onboarding_class,
    n_ga_wallets_new,
    n_ga_wallets_cumulative
FROM `dbt`.`fct_execution_gnosis_app_gpay_wallets_daily`
ORDER BY date, onboarding_class