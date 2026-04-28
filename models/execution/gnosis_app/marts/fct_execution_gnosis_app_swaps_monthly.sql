{{
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='(month)',
    tags=['production','execution','gnosis_app','cow','swaps','mart']
  )
}}

SELECT
    toStartOfMonth(block_timestamp)              AS month,
    count(*)                                     AS n_swaps,
    countIf(was_filled)                          AS n_swaps_filled,
    countIf(NOT was_filled)                      AS n_swaps_unfilled,
    countDistinct(taker)                         AS n_swappers,
    countDistinct(order_uid)                     AS n_orders,
    sum(amount_usd)                              AS volume_usd_filled
FROM {{ ref('int_execution_gnosis_app_swaps') }}
WHERE toStartOfMonth(block_timestamp) < toStartOfMonth(today())
GROUP BY month
ORDER BY month
