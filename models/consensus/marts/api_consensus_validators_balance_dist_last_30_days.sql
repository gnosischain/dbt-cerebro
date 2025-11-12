{{
    config(
        materialized='view',
        tags=["production", "consensus", "validators_apy"]
    )
}}

SELECT
    date,
    q05_balance AS q05,
    q10_balance AS q10,
    q25_balance AS q25,
    q50_balance AS q50,
    q75_balance AS q75,
    q90_balance AS q90,
    q95_balance AS q95
FROM {{ ref('fct_consensus_validators_dists_last_30_days') }}