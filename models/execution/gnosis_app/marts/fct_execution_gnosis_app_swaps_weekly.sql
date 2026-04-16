{{
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='(week)',
    tags=['production','execution','gnosis_app','cow','swaps','mart']
  )
}}

{# Description in schema.yml — see fct_execution_gnosis_app_swaps_weekly #}

SELECT
    toStartOfWeek(block_timestamp, 1)            AS week,
    count(*)                                     AS n_swaps,
    countIf(was_filled)                          AS n_swaps_filled,
    countIf(NOT was_filled)                      AS n_swaps_unfilled,
    countDistinct(taker)                         AS n_swappers,
    countDistinct(order_uid)                     AS n_orders,
    sum(amount_usd)                              AS volume_usd_filled
FROM {{ ref('int_execution_gnosis_app_swaps') }}
GROUP BY week
ORDER BY week
