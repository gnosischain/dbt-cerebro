{{
    config(
        materialized='view',
        tags=["production", "consensus", 'tier0', 'api:staked_gno', 'granularity:latest']
    )
}}

SELECT sub.*, (SELECT toDate(max(date)) FROM {{ ref('int_consensus_deposits_withdrawals_daily') }}) AS as_of_date
FROM (
SELECT
    toUInt32(value) AS value
    ,change_pct
FROM 
    {{ ref('fct_consensus_info_latest') }}
WHERE
    label = 'Staked'
) AS sub
