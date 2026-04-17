{{
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='(date, solver)',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','gnosis_app','cow','swaps','mart']
  )
}}

SELECT
    toDate(block_timestamp)                      AS date,
    solver                                       AS solver,
    count(*)                                     AS n_swaps_filled,
    countDistinct(taker)                         AS n_swappers,
    sum(amount_usd)                              AS volume_usd_filled
FROM {{ ref('int_execution_gnosis_app_swaps') }}
WHERE was_filled = 1
GROUP BY toDate(block_timestamp), solver
ORDER BY date, solver
