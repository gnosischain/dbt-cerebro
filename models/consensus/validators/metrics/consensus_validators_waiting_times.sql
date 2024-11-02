{{ 
    config(
        materialized='table',
    ) 
}}

{% set delay_fields = [
    {'field': 'eligibility_delay', 'label': 'Eligibility'},
    {'field': 'activation_delay', 'label': 'Activation'},
    {'field': 'entry_delay', 'label': 'Entry'},
    {'field': 'exit_activation_delay', 'label': 'De-activation'},
    {'field': 'exit_withdrawable_delay', 'label': 'Withdrawable'},
    {'field': 'queue_exit_delay', 'label': 'Exit'},
    {'field': 'exit_delay', 'label': 'Exit Delay'}
] %}

{% set aggregations = [
    {'func': 'min', 'label': 'Min'},
    {'func': 'max', 'label': 'Max'},
    {'func': 'median', 'label': 'Median'},
    {'func': 'avg', 'label': 'Mean'}
] %}

WITH final AS (
    SELECT
        f_validator_pubkey,
        MIN(f_eth1_block_timestamp) AS eth1_block_timestamp,
        MAX(toInt64(activation_eligibility_time) - toInt64(f_eth1_block_timestamp)) AS eligibility_delay,
        MAX(toInt64(activation_time) - toInt64(activation_eligibility_time)) AS activation_delay,
        MAX(toInt64(activation_time) - toInt64(f_eth1_block_timestamp)) AS entry_delay,
        MAX(toInt64(exit_time) - toInt64(exit_request_time)) AS exit_activation_delay,
        MAX(toInt64(withdrawable_time) - toInt64(exit_time)) AS exit_withdrawable_delay,
        MAX(toInt64(withdrawable_time) - toInt64(exit_request_time)) AS queue_exit_delay,
        MAX(toInt64(withdrawable_time) - toInt64(exit_voluntary_time)) AS exit_delay
    FROM 
        {{ ref('consensus_validators_queue') }}  
    GROUP BY
        f_validator_pubkey
)

{% set union_queries = [] %}
{% for delay in delay_fields %}
    {% for agg in aggregations %}
        {% set query %}
            SELECT 
                toDate(eth1_block_timestamp) AS day, 
                COALESCE(toFloat64({{ agg.func }}({{ delay.field }})), 0) / 3600 AS value, 
                '{{ agg.label }} {{ delay.label }}' AS label 
            FROM final 
            WHERE toDate(eth1_block_timestamp) < (SELECT MAX(toDate(eth1_block_timestamp)) FROM final)
            GROUP BY day
        {% endset %}
        {% do union_queries.append(query) %}
    {% endfor %}
{% endfor %}

{{ union_queries | join('\nUNION ALL\n') }}