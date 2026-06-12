{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        on_schema_change='sync_all_columns',
        engine='ReplacingMergeTree()',
        order_by='(date, protocol, container_address, ubo_address, token_address)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        pre_hook=[
            "SET max_memory_usage = 6000000000",
            "SET max_bytes_before_external_group_by = 2000000000",
            "SET max_bytes_before_external_sort = 2000000000"
        ],
        post_hook=[
            "SET max_memory_usage = 0",
            "SET max_bytes_before_external_group_by = 0",
            "SET max_bytes_before_external_sort = 0"
        ],
        tags=['production','execution','ubo','claims','supply_claims']
    )
}}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}

-- Rows from fct_ubo_supply_claims_daily where ubo_address is itself a known
-- container for the bridge token (container_address). Materialized so that
-- fct_ubo_supply_claims_resolved_daily can place this tiny table on the right
-- (hash-table) side of the redistribution join, streaming the full claims
-- dataset through on the left (probe) side without loading it into memory.
SELECT
    f.date, f.protocol, f.container_address, f.ubo_address, f.token_address,
    f.symbol, f.token_class, f.balance_raw, f.balance, f.balance_usd
FROM {{ ref('fct_ubo_supply_claims_daily') }} f
INNER JOIN {{ ref('fct_ubo_known_containers_daily') }} kc
    ON  f.date              = kc.date
    AND f.ubo_address       = kc.container_address
    AND f.container_address = kc.token_address
WHERE f.date < today()
  {% if start_month and end_month %}
    AND toStartOfMonth(f.date) >= toDate('{{ start_month }}')
    AND toStartOfMonth(f.date) <= toDate('{{ end_month }}')
  {% else %}
    {{ apply_monthly_incremental_filter('f.date', 'date', 'true') }}
  {% endif %}
