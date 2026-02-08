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

{% set blocks_pre_filter %}
    block_timestamp > '1970-01-01'
    {{ apply_monthly_incremental_filter('block_timestamp', 'date', add_and=True) }}
{% endset %}

WITH deduped_blocks AS (
    {{ dedup_source(
        source_ref=source('execution', 'blocks'),
        partition_by='block_number',
        columns='block_timestamp, gas_used, gas_limit',
        pre_filter=blocks_pre_filter
    ) }}
)

SELECT
  toDate(block_timestamp)         AS date,
  SUM(gas_used)                   AS gas_used_sum,
  SUM(gas_limit)                  AS gas_limit_sum,
  gas_used_sum / NULLIF(gas_limit_sum, 0) AS gas_used_fraq
FROM deduped_blocks
GROUP BY date