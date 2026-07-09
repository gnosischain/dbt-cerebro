{{
    config(
        materialized='view',
        tags=["production", "consensus", 'tier1', 'api:validators_balances_distribution', 'granularity:daily']
    )
}}

-- in GNO (int_consensus_validators_dists_daily already converts at the origin)
SELECT
    date,
    q05_balance AS q05,
    q10_balance AS q10,
    q25_balance AS q25,
    q50_balance AS q50,
    q75_balance AS q75,
    q90_balance AS q90,
    q95_balance AS q95
FROM {{ ref('int_consensus_validators_dists_daily') }}
ORDER BY date ASC