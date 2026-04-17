

SELECT
    offer_name,
    gateway_address,
    created_at,
    total_buys,
    total_payers,
    first_buy_at,
    last_buy_at
FROM `dbt`.`fct_execution_gnosis_app_marketplace_offers_latest`
ORDER BY total_buys DESC