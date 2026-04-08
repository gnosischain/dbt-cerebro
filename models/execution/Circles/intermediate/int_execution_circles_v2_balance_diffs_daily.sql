{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if var('start_month', none) else 'delete+insert'),
        on_schema_change='sync_all_columns',
        engine='ReplacingMergeTree()',
        order_by='(date, account, token_address)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles_v2', 'balances']
    )
}}

{% set start_month = var('start_month', none) %}
{% set end_month = var('end_month', none) %}
SELECT
    toDate(block_timestamp) AS date,
    account,
    token_address,
    max(circles_type) AS circles_type,
    sum(delta_raw) AS delta_raw,
    max(toUInt64(toUnixTimestamp(block_timestamp))) AS last_activity_ts
FROM (
    -- Debit
    SELECT
        block_timestamp,
        from_address AS account,
        token_address,
        circles_type,
        -toInt256(amount_raw) AS delta_raw
    FROM {{ ref('int_execution_circles_v2_transfers') }}
    WHERE 1 = 1
      {% if start_month and end_month %}
        AND toStartOfMonth(toDate(block_timestamp)) >= toDate('{{ start_month }}')
        AND toStartOfMonth(toDate(block_timestamp)) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter(source_field='block_timestamp', destination_field='date', add_and=true) }}
      {% endif %}

    UNION ALL

    -- Credit
    SELECT
        block_timestamp,
        to_address AS account,
        token_address,
        circles_type,
        toInt256(amount_raw) AS delta_raw
    FROM {{ ref('int_execution_circles_v2_transfers') }}
    WHERE 1 = 1
      {% if start_month and end_month %}
        AND toStartOfMonth(toDate(block_timestamp)) >= toDate('{{ start_month }}')
        AND toStartOfMonth(toDate(block_timestamp)) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter(source_field='block_timestamp', destination_field='date', add_and=true) }}
      {% endif %}
)
GROUP BY 1, 2, 3
