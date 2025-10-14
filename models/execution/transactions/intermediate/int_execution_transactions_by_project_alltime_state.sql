{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    engine='AggregatingMergeTree()',
    order_by='(project, month)',
    partition_by='toStartOfMonth(month)',
    unique_key='(project, month)',
    tags=['production','execution','transactions']
  )
}}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}

WITH src AS (
  SELECT
    toStartOfMonth(date)                         AS month,
    project,
    sumState(tx_count)                           AS txs_state,
    sumState(fee_native_sum)                     AS fee_state,
    groupBitmapMergeState(ua_bitmap_state)       AS aa_state
  FROM {{ ref('int_execution_transactions_by_project_daily') }}
  WHERE 1 = 1
    {% if start_month and end_month %}
      AND toStartOfMonth(date) >= toDate('{{ start_month }}')
      AND toStartOfMonth(date) <= toDate('{{ end_month }}')
    {% endif %}
    {% if is_incremental() and not (start_month and end_month) %}
      AND toStartOfMonth(date) >= toStartOfMonth(addMonths(today(), -2))
    {% endif %}
  GROUP BY month, project
)

SELECT project, month, txs_state, fee_state, aa_state
FROM src