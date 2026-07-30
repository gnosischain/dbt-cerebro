{% set start_month = var('start_month', none) %}
{% set end_month = var('end_month', none) %}
{% set validator_index_start = var('validator_index_start', none) %}
{% set validator_index_end = var('validator_index_end', none) %}

{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if start_month else 'insert_overwrite'),
        engine='ReplacingMergeTree()',
        order_by='(date, validator_index)',
        partition_by='toStartOfMonth(date)',
        pre_hook=[
            "SET max_bytes_before_external_group_by = 2000000000",
            "SET max_bytes_before_external_sort = 2000000000"
        ],
        post_hook=[
            "SET max_bytes_before_external_group_by = 0",
            "SET max_bytes_before_external_sort = 0"
        ],
        tags=["production", "consensus", "validators_withdrawals", "microbatch"]
    )
}}

{% set range_sql %}
  {% if validator_index_start is not none and validator_index_end is not none %}
    AND validator_index >= {{ validator_index_start }}
    AND validator_index < {{ validator_index_end }}
  {% endif %}
{% endset %}

-- withdrawals_amount_gno is REAL GNO: source amounts are gwei-of-mGNO
-- (32 mGNO = 1 GNO), converted here at the origin via /1e9/32.
-- Consumers must NOT divide by 32 again.
--
-- incremental_strategy resolves to `append` when start_month is set: refresh.py
-- runs validator-index STAGES within each month, and insert_overwrite would make
-- every stage's REPLACE PARTITION wipe the previous stages' rows (verified
-- 2026-07-09: a staged insert_overwrite rebuild left only the 500k-600k stage).
-- Same design as int_consensus_validators_income_daily.
SELECT
    toStartOfDay(slot_timestamp) AS date
    ,validator_index
    ,SUM(amount) / POWER(10, 9) / 32 AS withdrawals_amount_gno
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
