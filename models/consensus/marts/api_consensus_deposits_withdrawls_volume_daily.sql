{{
    config(
        materialized='view',
        tags=["production", "consensus", "deposits_withdrawals", 'tier1', 'api: deposits_withdrawals_volume_d']
    )
}}

SELECT
    date
    ,label
    ,total_amount AS value
FROM 
    {{ ref('int_consensus_deposits_withdrawals_daily') }}
ORDER BY date, label