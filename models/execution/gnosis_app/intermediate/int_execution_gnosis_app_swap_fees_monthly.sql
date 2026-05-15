{{
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='month',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','gnosis_app','swap_fees','monthly']
  )
}}

WITH rolled AS (
    SELECT
        toStartOfMonth(date)        AS month,
        sum(n_filled_swaps)         AS n_filled_swaps,
        sum(volume_usd)             AS volume_usd,
        sum(fee_native_total)       AS fee_native_total,
        sum(fee_usd_total)          AS fee_usd_total
    FROM {{ ref('int_execution_gnosis_app_swap_fees_daily') }}
    WHERE date < toStartOfMonth(today())
    GROUP BY month
)

SELECT
    month,
    n_filled_swaps,
    volume_usd,
    fee_native_total,
    fee_usd_total,
    round(fee_usd_total / nullIf(volume_usd, 0) * 100, 4) AS fee_pct_of_volume
FROM rolled
ORDER BY month
