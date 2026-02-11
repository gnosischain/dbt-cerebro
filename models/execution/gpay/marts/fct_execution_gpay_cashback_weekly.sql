{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(week)',
    tags=['production','execution','gpay']
  )
}}

WITH weekly AS (
    SELECT
        toStartOfWeek(date, 1)               AS week,
        sum(amount)                           AS amount_gno,
        round(toFloat64(sum(amount_usd)), 2)  AS amount_usd
    FROM {{ ref('int_execution_gpay_cashback_daily') }}
    WHERE toStartOfWeek(date, 1) < toStartOfWeek(today(), 1)
    GROUP BY week
)

SELECT
    week,
    amount_gno,
    amount_usd,
    sum(amount_gno) OVER (ORDER BY week) AS cumulative_gno,
    sum(amount_usd) OVER (ORDER BY week) AS cumulative_usd
FROM weekly
ORDER BY week
