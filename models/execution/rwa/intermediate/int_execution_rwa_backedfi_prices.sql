{{ 
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        engine='ReplacingMergeTree()',
        order_by='(date, bticker)',
        partition_by='toStartOfMonth(date)',
        pre_hook=[
            "SET max_memory_usage = 6000000000",
            "SET max_bytes_before_external_group_by = 2000000000"
        ],
        post_hook=[
            "SET max_memory_usage = 0",
            "SET max_bytes_before_external_group_by = 0"
        ],
        tags=['production','execution','rwa','backedfi','prices']
    )
}}


{% set btickers = [
    'bC3M',
    'bCOIN',
    'bCSPX',
    'bHIGH',
    'bIB01',
    'bIBTA',
    'bMSTR',
    'bNVDA',
    'TSLAx'
] %}

{% set sql_queries = [] %}

{% for bticker in btickers %}
    {% set model_name = 'contracts_backedfi_' + bticker + '_Oracle_events' %}
    {% set sql %}
        SELECT
            '{{ bticker }}' as bticker
            ,toStartOfDay(block_timestamp) AS date
            ,argMax(toUInt256OrNull(decoded_params['current'])/POWER(10,8),block_timestamp) AS price
        FROM {{ ref(model_name) }}
        WHERE
            event_name = 'AnswerUpdated'
            AND block_timestamp < today()
            {% if var('start_month', none) and var('end_month', none) %}
                AND toStartOfMonth(toDate(block_timestamp)) >= toDate('{{ var("start_month") }}')
                AND toStartOfMonth(toDate(block_timestamp)) <= toDate('{{ var("end_month") }}')
            {% else %}
                {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true') }}
            {% endif %}
        GROUP BY 1, 2
    {% endset %}
    {% do sql_queries.append(sql) %}
{% endfor %}

{{ sql_queries | join('\nunion all\n') }}