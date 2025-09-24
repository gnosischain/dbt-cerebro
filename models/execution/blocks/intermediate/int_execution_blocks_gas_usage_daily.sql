{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    engine='ReplacingMergeTree()',
    order_by='(day)',
    unique_key='(day)',
    partition_by='toStartOfMonth(day)',
    tags=['production','execution','transactions','gas']
  )
}}

SELECT
  toDate(block_timestamp)         AS day,
  SUM(toFloat64OrZero(gas_used))  AS gas_used_sum,
  SUM(toFloat64OrZero(gas_limit)) AS gas_limit_sum
FROM {{ source('execution','blocks') }}
{% if is_incremental() %}
WHERE block_timestamp >= date_trunc('month', now())
{% endif %}
GROUP BY day