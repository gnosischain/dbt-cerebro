

SELECT
    date,
    token_bought_symbol,
    n_topups,
    n_ga_users,
    n_gp_wallets,
    round(toFloat64(volume_token_bought), 6)  AS volume_token_bought,
    round(toFloat64(volume_usd), 2)           AS volume_usd
FROM `dbt`.`fct_execution_gnosis_app_gpay_topups_by_token_daily`
ORDER BY date, token_bought_symbol