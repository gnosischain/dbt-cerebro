

SELECT
    date,
    offer_address,
    n_claims,
    n_claimers,
    amount_received_usd,
    n_active_pool_30d,
    claim_rate_pct
FROM `dbt`.`int_execution_gnosis_app_token_offer_claim_funnel_daily`
WHERE date < today()
ORDER BY date DESC, n_claims DESC