{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='(token_address, date)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles_v2', 'supply_daily']
    )
}}


SELECT
    date,
    token_address,
    -balance_raw AS supply_raw,
    -balance_raw/POWER(10,18) AS supply,
    -demurraged_balance_raw/POWER(10,18) AS demurraged_supply
FROM {{ ref('int_execution_circles_v2_balances_daily') }}
WHERE account = '0x0000000000000000000000000000000000000000'
