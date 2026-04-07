{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date, balance_bucket)',
        partition_by='toStartOfMonth(date)',
        unique_key='(date, balance_bucket)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles_v2', 'balance_cohorts']
    )
}}

WITH balances AS (
    SELECT
        date,
        account,
        toFloat64(balance_raw) / 1e18 AS balance,
        toFloat64(demurraged_balance_raw) / 1e18 AS demurraged_balance
    FROM {{ ref('int_execution_circles_v2_balances_daily') }}
    WHERE account != '0x0000000000000000000000000000000000000000'
      AND balance_raw > toInt256(0)
      AND date < today()
      {% if is_incremental() %}
        AND date >= (SELECT max(date) - 1 FROM {{ this }})
      {% endif %}
),
bucketed AS (
    SELECT
        date,
        account,
        balance,
        demurraged_balance,
        multiIf(
            balance < 1,       '0-1',
            balance < 10,      '1-10',
            balance < 100,     '10-100',
            balance < 1000,    '100-1k',
            balance < 10000,   '1k-10k',
            balance < 100000,  '10k-100k',
                               '100k+'
        ) AS balance_bucket
    FROM balances
)

SELECT
    date,
    balance_bucket,
    count() AS holder_count,
    sum(balance) AS total_balance,
    sum(demurraged_balance) AS total_demurraged_balance
FROM bucketed
GROUP BY date, balance_bucket
ORDER BY date, balance_bucket
