{{
  config(
    materialized='view',
    tags=['production','execution','gnosis_app','tier1','api:gnosis_app_swap_fees','granularity:daily']
  )
}}

SELECT
    date,
    n_filled_swaps,
    n_distinct_takers,
    volume_usd,
    fee_native_total,
    fee_usd_total,
    fee_pct_of_volume
FROM {{ ref('int_execution_gnosis_app_swap_fees_daily') }}
WHERE date < today()
ORDER BY date DESC
