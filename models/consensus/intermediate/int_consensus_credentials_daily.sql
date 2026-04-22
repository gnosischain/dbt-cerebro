{% set start_month = var('start_month', none) %}
{% set end_month = var('end_month', none) %}

{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if start_month else 'delete+insert'),
        engine='ReplacingMergeTree()',
        order_by='(date,credentials_type)',
        unique_key='(date,credentials_type)',
        partition_by='toStartOfMonth(date)',
        tags=["production", "consensus", "credentials"]
    )
}}


SELECT
    toStartOfDay(slot_timestamp) AS date
    ,leftUTF8(withdrawal_credentials, 4) AS credentials_type
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
