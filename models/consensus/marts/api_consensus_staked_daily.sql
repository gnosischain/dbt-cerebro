{{
    config(
        materialized='view',
        tags=["production", "consensus", 'tier1', 'api:staked_gno', 'granularity:daily']
    )
}}

SELECT
    date
    ,effective_balance AS value
FROM {{ ref('int_consensus_validators_balances_daily') }}
ORDER BY date