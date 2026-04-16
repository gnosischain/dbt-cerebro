{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='(avatar, date, token_address)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles_v2', 'balances']
    )
}}

SELECT
    account AS avatar,
    date,
    token_address,
    toFloat64(balance_raw) / pow(10, 18) AS balance,
    toFloat64(demurraged_balance_raw) / pow(10, 18) AS balance_demurraged
FROM {{ ref('int_execution_circles_v2_balances_daily') }}
WHERE balance_raw > POW(10, 15)
  AND account != '0x0000000000000000000000000000000000000000'
