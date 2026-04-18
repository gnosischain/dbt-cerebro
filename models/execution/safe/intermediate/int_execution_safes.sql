{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if start_month else 'delete+insert'),
    engine='ReplacingMergeTree()',
    order_by='(safe_address)',
    partition_by='toStartOfMonth(block_timestamp)',
    unique_key='(safe_address)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','execution','safe'],
    pre_hook=["SET join_algorithm = 'grace_hash'"],
    post_hook=["SET join_algorithm = 'default'"]
  )
}}
{# execution.traces stores addresses + calldata as lowercase hex WITHOUT a
   '0x' prefix. Strip the prefix from the seed values when comparing. #}
{% set traces_pre_filter %}
    action_call_type = 'delegate_call'
    AND result_gas_used > 10000
    AND lower(substring(action_input, 1, 8)) IN ('0ec78d9e','a97ab18a','b63e800d')
    AND lower(action_to) IN (
        SELECT lower(replaceAll(address, '0x', '')) FROM {{ ref('safe_singletons') }}
    )
    AND block_timestamp >= toDateTime('2020-05-21')
    {% if start_month and end_month %}
      AND toStartOfMonth(toDate(block_timestamp)) >= toDate('{{ start_month }}')
      AND toStartOfMonth(toDate(block_timestamp)) <= toDate('{{ end_month }}')
    {% else %}
      {{ apply_monthly_incremental_filter('block_timestamp', 'block_timestamp', add_and=True) }}
    {% endif %}
{% endset %}

WITH traces AS (
    {{ dedup_source(
        source_ref   = source('execution','traces'),
        partition_by = 'block_number, transaction_hash, trace_address',
        columns      = 'action_from, action_to, action_input, action_call_type, result_gas_used, block_timestamp, block_number, transaction_hash',
        pre_filter   = traces_pre_filter
    ) }}
),

singletons AS (
    -- Match the trace storage format: lowercase hex, NO 0x prefix.
    SELECT
        lower(replaceAll(address, '0x', ''))        AS singleton_address,
        version,
        is_l2,
        lower(replaceAll(setup_selector, '0x', '')) AS setup_selector
    FROM {{ ref('safe_singletons') }}
)

SELECT
    -- Re-prefix outputs so downstream models get the canonical 0x... shape.
    concat('0x', lower(tr.action_from))              AS safe_address,
    sg.version                                       AS creation_version,
    sg.is_l2                                         AS is_l2,
    concat('0x', lower(tr.action_to))                AS creation_singleton,
    toDate(tr.block_timestamp)                       AS block_date,
    tr.block_timestamp                               AS block_timestamp,
    tr.block_number                                  AS block_number,
    concat('0x',tr.transaction_hash)                 AS tx_hash,
    tr.result_gas_used                               AS gas_used
FROM traces tr
INNER JOIN singletons sg
    ON lower(tr.action_to) = sg.singleton_address
   AND lower(substring(tr.action_input, 1, 8)) = sg.setup_selector
