{% set start_month = var('start_month', none) %}
{% set end_month = var('end_month', none) %}
{% set validator_index_start = var('validator_index_start', none) %}
{% set validator_index_end = var('validator_index_end', none) %}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        engine='ReplacingMergeTree()',
        order_by='(date, validator_index)',
        partition_by='toStartOfMonth(date)',
        tags=["production", "consensus", "validators_withdrawals"]
    )
}}

{% set range_sql %}
  {% if validator_index_start is not none and validator_index_end is not none %}
    AND validator_index >= {{ validator_index_start }}
    AND validator_index < {{ validator_index_end }}
  {% endif %}
{% endset %}

-- NOTE: withdrawals_amount_gno is actually mGNO-denominated (32 mGNO = 1 real GNO;
-- see the unit warning in int_consensus_validators_income_daily.sql). Only
-- consumer today (income_daily) passes it through to columns that already get
-- the extra /32 downstream — if you add a NEW consumer that displays this
-- column directly, you need to divide by 32 yourself.
SELECT
    toStartOfDay(slot_timestamp) AS date
    ,validator_index
    ,SUM(amount) / POWER(10, 9) AS withdrawals_amount_gno
    ,COUNT(*) AS withdrawals_count
FROM {{ ref('stg_consensus__withdrawals') }}
WHERE
    slot_timestamp < today()
    {% if start_month and end_month %}
    AND toStartOfMonth(slot_timestamp) >= toDate('{{ start_month }}')
    AND toStartOfMonth(slot_timestamp) <= toDate('{{ end_month }}')
    {% else %}
    {{ apply_monthly_incremental_filter('slot_timestamp', 'date', 'true', lookback_days=2, filters_sql=range_sql) }}
    {% endif %}
    {% if validator_index_start is not none and validator_index_end is not none %}
    AND validator_index >= {{ validator_index_start }}
    AND validator_index < {{ validator_index_end }}
    {% endif %}
GROUP BY 1, 2
