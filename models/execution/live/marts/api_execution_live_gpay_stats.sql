{{
    config(
        materialized='view',
        tags=['live', 'execution', 'gpay', 'api']
    )
}}

-- One-row KPIs for the live GP pulse, matching the feed population.

SELECT
    countIf(action = 'Payment')                                              AS payment_count,
    countIf(action = 'Fiat Top Up')                                          AS topup_count,
    round(coalesce(sumIf(amount_usd, action = 'Payment'), 0), 0)             AS payment_volume_usd,
    round(coalesce(sumIf(amount_usd, action = 'Fiat Top Up'), 0), 0)         AS topup_volume_usd,
    uniqExactIf(wallet_address, action = 'Payment')                          AS unique_payers,
    uniqExact(wallet_address)                                                AS unique_wallets
FROM {{ ref('api_execution_live_gpay_activity') }}
