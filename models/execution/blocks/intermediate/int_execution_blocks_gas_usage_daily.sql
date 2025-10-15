{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    engine='ReplacingMergeTree()',
    order_by='(date)',
    unique_key='(date)',
    partition_by='toStartOfMonth(date)',
    tags=['production','execution','blocks','gas']
  )
}}

SELECT
  toDate(block_timestamp)         AS date,
  SUM(gas_used)                   AS gas_used_sum,
  SUM(gas_limit)                  AS gas_limit_sum,
  gas_used_sum / NULLIF(gas_limit_sum, 0) AS gas_used_fraq
FROM {{ ref('stg_execution__blocks') }}
{{ apply_monthly_incremental_filter('block_timestamp', 'date') }}
GROUP BY date