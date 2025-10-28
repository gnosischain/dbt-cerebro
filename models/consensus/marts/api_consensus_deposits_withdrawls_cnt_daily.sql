{{
    config(
        materialized='view',
        tags=["production", "consensus", "deposits_withdrawals"]
    )
}}

SELECT
    date
    ,label
    ,cnt AS value
FROM 
    {{ ref('int_consensus_deposits_withdrawals_daily') }}
ORDER BY date, label