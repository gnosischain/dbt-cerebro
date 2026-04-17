

SELECT
    date,
    offer_name,
    n_buys,
    n_new_payers,
    cumulative_buys,
    cumulative_payers
FROM `dbt`.`fct_execution_gnosis_app_marketplace_buys_cumulative_daily`
ORDER BY date, offer_name