{% set start_month = var('start_month', none) %}
{% set end_month = var('end_month', none) %}

{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if start_month else 'delete+insert'),
        engine='ReplacingMergeTree()',
        order_by='(date, status)',
        unique_key='(date, status)',
        partition_by='toStartOfMonth(date)',
        tags=["production", "consensus", "validators_status"]
    )
}}


SELECT
    toStartOfDay(slot_timestamp) AS date
    ,status
    ,COUNT(*) AS cnt
FROM {{ ref('stg_consensus__validators') }}
WHERE
    slot_timestamp < today()
    {% if start_month and end_month %}
    AND toStartOfMonth(slot_timestamp) >= toDate('{{ start_month }}')
    AND toStartOfMonth(slot_timestamp) <= toDate('{{ end_month }}')
    {% else %}
    {{ apply_monthly_incremental_filter('slot_timestamp', 'date', 'true') }}
    {% endif %}
GROUP BY 1, 2
