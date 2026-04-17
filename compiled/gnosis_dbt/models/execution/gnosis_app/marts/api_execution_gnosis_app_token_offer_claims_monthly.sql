

SELECT
    month,
    n_claims,
    n_claimers,
    n_offers,
    round(toFloat64(volume_received_token), 6)   AS volume_received_token,
    round(toFloat64(volume_received_usd), 2)     AS volume_received_usd,
    round(toFloat64(volume_spent_crc), 2)        AS volume_spent_crc
FROM `dbt`.`fct_execution_gnosis_app_token_offer_claims_monthly`
ORDER BY month