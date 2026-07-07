{{
    config(
        materialized='view',
        tags=["production", "consensus", 'tier0', 'api:validators_balance_dististribution', 'granularity:last_30d']
    )
}}

-- NOTE: fct_consensus_validators_dists_last_30_days' balance quantiles are actually
-- mGNO-denominated (Gnosis Beacon Chain mirrors Ethereum's 32-unit-per-validator
-- convention; 32 mGNO = 1 real GNO). Divided by 32 below to convert to real GNO.
SELECT
    date,
    q05_balance / 32 AS q05,
    q10_balance / 32 AS q10,
    q25_balance / 32 AS q25,
    q50_balance / 32 AS q50,
    q75_balance / 32 AS q75,
    q90_balance / 32 AS q90,
    q95_balance / 32 AS q95
FROM {{ ref('fct_consensus_validators_dists_last_30_days') }}