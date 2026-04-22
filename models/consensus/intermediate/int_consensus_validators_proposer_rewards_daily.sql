{% set start_month = var('start_month', none) %}
{% set end_month = var('end_month', none) %}
{% set validator_index_start = var('validator_index_start', none) %}
{% set validator_index_end = var('validator_index_end', none) %}

{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if start_month else 'delete+insert'),
        engine='ReplacingMergeTree()',
        order_by='(date, validator_index)',
        unique_key='(date, validator_index)',
        partition_by='toStartOfMonth(date)',
        tags=["production", "consensus", "proposer_rewards"]
    )
}}

{% set range_sql %}
  {% if validator_index_start is not none and validator_index_end is not none %}
    AND validator_index >= {{ validator_index_start }}
    AND validator_index < {{ validator_index_end }}
  {% endif %}
{% endset %}

SELECT
    toStartOfDay(slot_timestamp) AS date
    ,proposer_index AS validator_index
    ,COUNT(*) AS proposed_blocks_count
    ,SUM(total) / POWER(10, 9) AS proposer_reward_total_gno
    ,SUM(attestations) / POWER(10, 9) AS proposer_reward_attestations_gno
    ,SUM(sync_aggregate) / POWER(10, 9) AS proposer_reward_sync_aggregate_gno
    ,SUM(proposer_slashings) / POWER(10, 9) AS proposer_reward_proposer_slashings_gno
    ,SUM(attester_slashings) / POWER(10, 9) AS proposer_reward_attester_slashings_gno
FROM {{ ref('stg_consensus__rewards') }}
WHERE
    slot_timestamp < today()
    {% if start_month and end_month %}
    AND toStartOfMonth(slot_timestamp) >= toDate('{{ start_month }}')
    AND toStartOfMonth(slot_timestamp) <= toDate('{{ end_month }}')
    {% else %}
    {{ apply_monthly_incremental_filter('slot_timestamp', 'date', 'true', lookback_days=2, filters_sql=range_sql) }}
    {% endif %}
    {% if validator_index_start is not none and validator_index_end is not none %}
    AND proposer_index >= {{ validator_index_start }}
    AND proposer_index < {{ validator_index_end }}
    {% endif %}
GROUP BY 1, 2
