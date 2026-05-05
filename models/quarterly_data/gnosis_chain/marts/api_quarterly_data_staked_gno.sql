{{
    config(
        materialized='view',
        tags=['production', 'quarterly_data', 'tier0', 'api:staked_gno', 'granularity:quarterly']
    )
}}

SELECT
    toStartOfQuarter(date) AS quarter,
    round(argMax(effective_balance, date) / 32, 1) AS staked_gno
FROM {{ ref('int_consensus_validators_balances_daily') }}
GROUP BY quarter
ORDER BY quarter
