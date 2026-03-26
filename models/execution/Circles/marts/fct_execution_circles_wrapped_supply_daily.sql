{{
    config(
        materialized='view',
        tags=['production', 'execution', 'circles', 'wrappers']
    )
}}

WITH supply_changes AS (
    SELECT
        toDate(block_timestamp) AS date,
        token_address,
        toInt256(amount_raw) AS supply_delta_raw
    FROM {{ ref('int_execution_circles_wrapper_transfers') }}
    WHERE from_address = {{ circles_zero_address() }}

    UNION ALL

    SELECT
        toDate(block_timestamp) AS date,
        token_address,
        -toInt256(amount_raw) AS supply_delta_raw
    FROM {{ ref('int_execution_circles_wrapper_transfers') }}
    WHERE to_address = {{ circles_zero_address() }}
),
daily_changes AS (
    SELECT
        date,
        token_address,
        sum(supply_delta_raw) AS supply_delta_raw
    FROM supply_changes
    GROUP BY 1, 2
)

SELECT
    date,
    token_address,
    supply_delta_raw,
    sum(supply_delta_raw) OVER (PARTITION BY token_address ORDER BY date) AS total_supply_raw
FROM daily_changes
