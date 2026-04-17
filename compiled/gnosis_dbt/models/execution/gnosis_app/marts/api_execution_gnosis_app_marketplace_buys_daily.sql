

SELECT
    date,
    offer_name,
    n_buys,
    n_payers,
    round(toFloat64(volume_token), 6)  AS volume_token
FROM `dbt`.`fct_execution_gnosis_app_marketplace_buys_daily`
ORDER BY date, offer_name