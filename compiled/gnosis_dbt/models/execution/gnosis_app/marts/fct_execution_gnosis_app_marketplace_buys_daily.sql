

SELECT
    toDate(block_timestamp)                  AS date,
    offer_name                               AS offer_name,
    count(*)                                 AS n_buys,
    countDistinct(payer)                     AS n_payers,
    sum(amount)                              AS volume_token
FROM `dbt`.`int_execution_gnosis_app_marketplace_payments`
GROUP BY toDate(block_timestamp), offer_name
ORDER BY date, offer_name