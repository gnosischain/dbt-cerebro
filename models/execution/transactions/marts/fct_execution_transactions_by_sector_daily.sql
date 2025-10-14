{{
  config(
    materialized='table',
    tags=['production','execution','transactions'],
    engine='ReplacingMergeTree()',
    partition_by=['toYYYYMM(date)'],
    order_by=['date','sector'],
    incremental_strategy='insert_overwrite',
    on_schema_change='sync_all_columns',
    unique_key=['date','sector']
  )
}}

{% set mf = apply_monthly_incremental_filter('date') %}

SELECT
    date,
    sector,
    groupBitmapMerge(ua_bitmap_state)                 AS active_accounts,
    sum(tx_count)                                     AS txs,
    sum(gas_used_sum)                                 AS gas_used_sum,
    round(toFloat64(sum(fee_native_sum)), 6)          AS fee_native_sum
FROM {{ ref('int_execution_transactions_by_project_daily') }}
WHERE 1 = 1
  {% if mf and mf | trim != '' %}
    AND {{ mf }}
  {% endif %}
  AND date < today()
GROUP BY
  date, sector