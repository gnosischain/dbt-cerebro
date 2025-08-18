{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date)',
        unique_key='(date)',
        partition_by='toStartOfMonth(date)',
        pre_hook=[
            "SET max_bytes_before_external_group_by = 1000000000",
            "SET max_bytes_before_external_sort = 1000000000",
            "SET max_block_size = 1024"
        ]
    ) 
}}

-- Step 1: Get daily rewards (this should be small since only proposers)
WITH daily_rewards AS (
    SELECT 
        toStartOfDay(slot_timestamp) AS date,
        SUM(total) AS total_rewards,
        COUNT(DISTINCT proposer_index) AS unique_proposers,
        -- Collect all proposer indices for this date
        groupArray(DISTINCT proposer_index) AS proposer_indices
    FROM {{ ref('stg_consensus__rewards') }}
    WHERE total > 0
    {{ apply_monthly_incremental_filter(source_field='slot_timestamp',destination_field='date',add_and='false') }}
    GROUP BY 1
),

-- Step 2: Get balances only for proposers on their respective dates
-- Use arrayJoin to expand the proposer arrays
proposer_balances AS (
    SELECT 
        dr.date,
        dr.total_rewards,
        dr.unique_proposers,
        SUM(v.balance) AS total_proposer_balance
    FROM daily_rewards dr
    ARRAY JOIN dr.proposer_indices AS proposer_idx
    LEFT JOIN (
        SELECT 
            toStartOfDay(slot_timestamp) AS date,
            validator_index,
            balance
        FROM {{ ref('stg_consensus__validators') }}
        WHERE balance > 0
        {{ apply_monthly_incremental_filter(source_field='slot_timestamp',destination_field='date',add_and='true') }}
    ) v ON v.date = dr.date AND v.validator_index = proposer_idx
    GROUP BY 1, 2, 3
)

-- Step 3: Calculate final metrics
SELECT 
    date,
    total_rewards AS amount,
    total_proposer_balance AS balance,
    unique_proposers,
    CASE 
        WHEN total_proposer_balance > total_rewards AND total_rewards > 0
        THEN total_rewards / (total_proposer_balance - total_rewards)
        ELSE 0 
    END AS rate,
    CASE 
        WHEN total_proposer_balance > total_rewards AND total_rewards > 0
        THEN floor(POWER((1 + total_rewards / (total_proposer_balance - total_rewards)), 365) - 1, 4) * 100
        ELSE 0 
    END AS apy
FROM proposer_balances