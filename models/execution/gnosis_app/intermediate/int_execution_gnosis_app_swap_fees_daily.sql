{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    engine='ReplacingMergeTree()',
    order_by='date',
    partition_by='toStartOfMonth(date)',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','gnosis_app','swap_fees','daily']
  )
}}

-- Daily aggregated CoW protocol fee revenue from Gnosis App swaps.
-- `fee_usd` is derived from `fee_amount` (denominated in the sold token)
-- pro-rated against the trade's USD value:
--    fee_usd = fee_amount / amount_sold * amount_usd
--
-- Rows are restricted to filled trades (was_filled = 1) so cancelled
-- pre-signatures don't show as zero-fee events.

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

SELECT
    toDate(first_fill_at)                                                   AS date,
    count()                                                                 AS n_filled_swaps,
    uniqExact(taker)                                                        AS n_distinct_takers,
    sum(toFloat64OrNull(toString(amount_usd)))                              AS volume_usd,
    sum(toFloat64OrNull(toString(fee_amount)))                              AS fee_native_total,
    sum(
        if(amount_sold > 0,
           toFloat64OrNull(toString(fee_amount))
             / toFloat64OrNull(toString(amount_sold))
             * toFloat64OrNull(toString(amount_usd)),
           toFloat64(0))
    )                                                                       AS fee_usd_total,
    round(
        sum(
            if(amount_sold > 0,
               toFloat64OrNull(toString(fee_amount))
                 / toFloat64OrNull(toString(amount_sold))
                 * toFloat64OrNull(toString(amount_usd)),
               toFloat64(0))
        )
        / nullIf(sum(toFloat64OrNull(toString(amount_usd))), 0) * 100,
        4
    )                                                                       AS fee_pct_of_volume
FROM {{ ref('int_execution_gnosis_app_swaps') }}
WHERE was_filled = 1
  AND first_fill_at IS NOT NULL
  AND first_fill_at < today()
  {% if start_month and end_month %}
    AND toStartOfMonth(toDate(first_fill_at)) >= toDate('{{ start_month }}')
    AND toStartOfMonth(toDate(first_fill_at)) <= toDate('{{ end_month }}')
  {% else %}
    {{ apply_monthly_incremental_filter(
          source_field='first_fill_at',
          destination_field='date',
          add_and=True) }}
  {% endif %}
GROUP BY date
