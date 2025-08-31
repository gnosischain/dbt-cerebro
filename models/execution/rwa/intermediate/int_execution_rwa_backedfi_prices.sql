{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date, bticker)',
        unique_key='(date, bticker)',
        partition_by='toStartOfMonth(date)',
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
            {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true') }}
        GROUP BY 1, 2
    {% endset %}
    {% do sql_queries.append(sql) %}
{% endfor %}

{{ sql_queries | join('\nunion all\n') }}