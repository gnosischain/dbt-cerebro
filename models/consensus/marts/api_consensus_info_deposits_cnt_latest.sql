{{
    config(
        materialized='view',
        tags=["production", "consensus", 'tier0', 'api:deposits_cnt', 'granularity:latest']
    )
}}

SELECT sub.*, (SELECT toDate(max(date)) FROM {{ ref('int_consensus_deposits_withdrawals_daily') }}) AS as_of_date
FROM (
SELECT
    value
    ,change_pct
FROM 
    {{ ref('fct_consensus_info_latest') }}
WHERE
    label = 'deposits_cnt'
) AS sub
