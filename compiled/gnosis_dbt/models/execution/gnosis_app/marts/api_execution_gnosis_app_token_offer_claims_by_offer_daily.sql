

SELECT
    date,
    offer_address,
    cycle_address,
    offer_token_symbol,
    n_claims,
    n_claimers,
    round(toFloat64(volume_received_token), 6)  AS volume_received_token,
    round(toFloat64(volume_received_usd), 2)    AS volume_received_usd,
    round(toFloat64(volume_spent_crc), 2)       AS volume_spent_crc,
    round(toFloat64(offer_price_in_crc), 6)     AS offer_price_in_crc
FROM `dbt`.`fct_execution_gnosis_app_token_offer_claims_by_offer_daily`
ORDER BY date, offer_address