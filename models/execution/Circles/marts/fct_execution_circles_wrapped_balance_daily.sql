{{
    config(
        materialized='view',
        tags=['production', 'execution', 'circles', 'wrappers']
    )
}}

WITH balance_deltas AS (
    SELECT
        toDate(block_timestamp) AS date,
        token_address,
        from_address AS account,
        -toInt256(amount_raw) AS delta
    FROM {{ ref('int_execution_circles_wrapper_transfers') }}

    UNION ALL

    SELECT
        toDate(block_timestamp) AS date,
        token_address,
        to_address AS account,
        toInt256(amount_raw) AS delta
    FROM {{ ref('int_execution_circles_wrapper_transfers') }}
),
daily_changes AS (
    SELECT
        date,
        token_address,
        account,
        sum(delta) AS balance_delta_raw
    FROM balance_deltas
    WHERE account != {{ circles_zero_address() }}
    GROUP BY 1, 2, 3
)

SELECT
    date,
    token_address,
    account,
    balance_delta_raw,
    sum(balance_delta_raw) OVER (PARTITION BY token_address, account ORDER BY date) AS balance_raw
FROM daily_changes
