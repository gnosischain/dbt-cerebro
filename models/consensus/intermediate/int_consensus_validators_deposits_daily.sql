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
        tags=["production", "consensus", "validators_deposits"]
    )
}}

{% set range_sql %}
  {% if validator_index_start is not none and validator_index_end is not none %}
    AND validator_index >= {{ validator_index_start }}
    AND validator_index < {{ validator_index_end }}
  {% endif %}
{% endset %}

WITH

labels AS (
    -- Use fct_consensus_validators_status_latest (reads stg_consensus__validators_all)
    -- instead of int_consensus_validators_labels — labels filters on balance > 0 and
    -- therefore drops exited validators, causing deposits to any such validator to be
    -- silently skipped.
    SELECT validator_index, lower(pubkey) AS pubkey
    FROM {{ ref('fct_consensus_validators_status_latest') }}
    WHERE 1=1
    {% if validator_index_start is not none and validator_index_end is not none %}
      AND validator_index >= {{ validator_index_start }}
      AND validator_index < {{ validator_index_end }}
    {% endif %}
),

beacon_deposits AS (
    SELECT
        toStartOfDay(d.slot_timestamp) AS date
        ,l.validator_index AS validator_index
        ,SUM(d.amount) AS amount_gwei
        ,COUNT(*) AS cnt
    FROM {{ ref('stg_consensus__deposits') }} d
    INNER JOIN labels l ON l.pubkey = lower(d.pubkey)
    WHERE
        d.slot_timestamp < today()
        {% if start_month and end_month %}
        AND toStartOfMonth(d.slot_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(d.slot_timestamp) <= toDate('{{ end_month }}')
        {% else %}
        {{ apply_monthly_incremental_filter('d.slot_timestamp', 'date', 'true', lookback_days=2, filters_sql=range_sql) }}
        {% endif %}
    GROUP BY 1, 2
),

request_deposits AS (
    SELECT
        toStartOfDay(r.slot_timestamp) AS date
        ,l.validator_index AS validator_index
        ,SUM(toUInt64(JSONExtractString(deposit, 'amount'))) AS amount_gwei
        ,COUNT() AS cnt
    FROM {{ ref('stg_consensus__execution_requests') }} r
    ARRAY JOIN JSONExtractArrayRaw(payload, 'deposits') AS deposit
    INNER JOIN labels l ON l.pubkey = lower(JSONExtractString(deposit, 'pubkey'))
    WHERE
        r.slot_timestamp < today()
        {% if start_month and end_month %}
        AND toStartOfMonth(r.slot_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(r.slot_timestamp) <= toDate('{{ end_month }}')
        {% else %}
        {{ apply_monthly_incremental_filter('r.slot_timestamp', 'date', 'true', lookback_days=2, filters_sql=range_sql) }}
        {% endif %}
    GROUP BY 1, 2
)

SELECT
    date
    ,validator_index
    ,SUM(amount_gwei) / POWER(10, 9) AS deposits_amount_gno
    ,SUM(cnt) AS deposits_count
FROM (
    SELECT * FROM beacon_deposits
    UNION ALL
    SELECT * FROM request_deposits
)
GROUP BY 1, 2
