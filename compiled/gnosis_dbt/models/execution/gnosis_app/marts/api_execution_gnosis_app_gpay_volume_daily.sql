

SELECT
    date,
    onboarding_class,
    funded_volume_usd,
    spend_usd,
    spend_count,
    spending_wallets,
    funded_volume_cumulative_usd,
    spend_cumulative_usd
FROM `dbt`.`fct_execution_gnosis_app_gpay_volume_daily`
ORDER BY date, onboarding_class