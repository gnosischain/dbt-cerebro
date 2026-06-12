{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(date, wrapper_address)',
    unique_key='(date, wrapper_address)',
    partition_by='toStartOfMonth(date)',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','circles_v2','wrappers','supply','daily']
  )
}}

-- Daily wrapped-supply per ERC-20 wrapper token.
--
-- wrap   = ERC-20 Transfer with from = 0x00..00 → +amount
-- unwrap = ERC-20 Transfer with to   = 0x00..00 → -amount
-- All other wrapper transfers are token re-shuffles between holders and
-- don't change total wrapped supply.
--
-- mint_delta is the per-day wrapped-supply delta; cumulative wrapped
-- supply is the prefix sum (computed downstream in the api_ view).

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

SELECT
    toDate(block_timestamp)                                                  AS date,
    token_address                                                            AS wrapper_address,
    sumIf(toFloat64(amount_raw) / pow(10, 18),
          from_address = '0x0000000000000000000000000000000000000000')       AS wrap_amount,
    sumIf(toFloat64(amount_raw) / pow(10, 18),
          to_address   = '0x0000000000000000000000000000000000000000')       AS unwrap_amount,
      sumIf(toFloat64(amount_raw) / pow(10, 18),
            from_address = '0x0000000000000000000000000000000000000000')
    - sumIf(toFloat64(amount_raw) / pow(10, 18),
            to_address   = '0x0000000000000000000000000000000000000000')     AS supply_delta
FROM {{ ref('int_execution_circles_v2_wrapper_transfers') }}
WHERE block_timestamp < today()
  {% if start_month and end_month %}
    AND toStartOfMonth(toDate(block_timestamp)) >= toDate('{{ start_month }}')
    AND toStartOfMonth(toDate(block_timestamp)) <= toDate('{{ end_month }}')
  {% else %}
    {{ apply_monthly_incremental_filter(
          source_field='block_timestamp',
          destination_field='date',
          add_and=True) }}
  {% endif %}
GROUP BY date, wrapper_address
