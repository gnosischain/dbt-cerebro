{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date)',
        unique_key='(date)',
        partition_by='toStartOfMonth(date)',
        tags=["production", "consensus", "validators_balances"]
    ) 
}}

SELECT
    date,
    q_balance[1] AS q05_balance,
    q_balance[2] AS q10_balance,
    q_balance[3] AS q25_balance,
    q_balance[4] AS q50_balance,
    q_balance[5] AS q75_balance,
    q_balance[6] AS q90_balance,
    q_balance[7] AS q95_balance,
    avg_balance,
    q_apy[1] AS q05_apy,
    q_apy[2] AS q10_apy,
    q_apy[3] AS q25_apy,
    q_apy[4] AS q50_apy,
    q_apy[5] AS q75_apy,
    q_apy[6] AS q90_apy,
    q_apy[7] AS q95_apy,
    avg_apy
FROM (
    SELECT
        date
        ,quantilesTDigest(
            0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95
        )(balance/POWER(10,9)) AS q_balance
        ,avg(balance/POWER(10,9)) AS avg_balance
        ,quantilesTDigest(
            0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95
        )(apy) AS q_apy
        ,avg(apy) AS avg_apy
    FROM {{ ref('int_consensus_validators_per_index_apy_daily') }}
    WHERE status != 'pending_queued' AND apy < 200 --outlier filter
    {{ apply_monthly_incremental_filter('date', 'date', 'true') }}
    GROUP BY date
)