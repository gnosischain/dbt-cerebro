{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if var('start_month', none) else 'delete+insert'),
    engine='ReplacingMergeTree()',
    order_by='(date, transfer_category)',
    unique_key='(date, transfer_category)',
    partition_by='toStartOfMonth(date)',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','circles_v2','transfers','daily']
  )
}}

-- Daily transfer volume + velocity by category. One row per
-- (date, transfer_category). Built directly off int_execution_circles_v2_transfers_categorised.

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

SELECT
    toDate(block_timestamp)                                  AS date,
    transfer_category                                        AS transfer_category,
    count()                                                  AS n_transfers,
    uniqExactIf(from_address,
        from_address != '0x0000000000000000000000000000000000000000') AS n_senders,
    uniqExactIf(to_address,
        to_address   != '0x0000000000000000000000000000000000000000') AS n_receivers,
    sum(toFloat64(amount_raw))            / pow(10, 18)      AS amount,
    sum(toFloat64(amount_demurraged_raw)) / pow(10, 18)      AS amount_demurraged
FROM {{ ref('int_execution_circles_v2_transfers_categorised') }}
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
GROUP BY date, transfer_category
