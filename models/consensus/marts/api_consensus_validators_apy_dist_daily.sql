{{
    config(
        materialized='view',
        tags=["production", "consensus", "validators_balances"]
    )
}}

-- in GNO
SELECT
    date,
    q05_apy AS q05,
    q10_apy AS q10,
    q25_apy AS q25,
    q50_apy AS q50,
    q75_apy AS q75,
    q90_apy AS q90,
    q95_apy AS q95
FROM {{ ref('int_consensus_validators_dists_daily') }}
ORDER BY date ASC