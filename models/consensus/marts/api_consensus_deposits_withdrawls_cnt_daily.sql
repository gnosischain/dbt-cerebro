{{
    config(
        materialized='view',
        tags=["production", "consensus", 'tier1', 'api:deposits_and_withdrawals', 'granularity:daily']
    )
}}

SELECT
    date
    ,label
    ,cnt AS value
FROM 
    {{ ref('int_consensus_deposits_withdrawals_daily') }}
ORDER BY date, label