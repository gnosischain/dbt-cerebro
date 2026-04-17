

SELECT
    toDate(c.block_timestamp)                        AS date,
    c.offer_address                                  AS offer_address,
    c.cycle_address                                  AS cycle_address,
    c.offer_token_symbol                             AS offer_token_symbol,
    count(*)                                         AS n_claims,
    countDistinct(c.ga_user)                         AS n_claimers,
    sum(c.amount_received)                           AS volume_received_token,
    sum(c.amount_received_usd)                       AS volume_received_usd,
    sum(c.amount_spent_crc)                          AS volume_spent_crc,
    any(c.offer_price_in_crc)                        AS offer_price_in_crc
FROM `dbt`.`int_execution_gnosis_app_token_offer_claims` c
GROUP BY date, offer_address, cycle_address, offer_token_symbol
ORDER BY date, offer_address