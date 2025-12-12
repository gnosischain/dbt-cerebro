{{
    config(
        materialized='view',
        tags=["production", "consensus", 'tier1', 'api: deposits_and_withdrawals_volume', 'granularity:daily']
    )
}}

SELECT
    date
    ,label
    ,total_amount AS value
FROM 
    {{ ref('int_consensus_deposits_withdrawals_daily') }}
ORDER BY date, label