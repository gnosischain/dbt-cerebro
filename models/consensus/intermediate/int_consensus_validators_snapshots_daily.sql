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
        tags=["production", "consensus", "validators_snapshots"]
    )
}}

{% set range_sql %}
  {% if validator_index_start is not none and validator_index_end is not none %}
    AND validator_index >= {{ validator_index_start }}
    AND validator_index < {{ validator_index_end }}
  {% endif %}
{% endset %}

-- Read through the staging view (FINAL handles source-side dedup). Target's
-- ReplacingMergeTree is the write-side safety net; no in-model aggregation.
SELECT
    toStartOfDay(slot_timestamp) AS date
    ,validator_index
    ,status
    ,lower(pubkey) AS pubkey
    ,lower(withdrawal_credentials) AS withdrawal_credentials
    ,if(
        startsWith(lower(withdrawal_credentials), '0x01')
        OR startsWith(lower(withdrawal_credentials), '0x02'),
        concat('0x', substring(lower(withdrawal_credentials), 27, 40)),
        NULL
    ) AS withdrawal_address
    ,balance AS balance_gwei
    ,effective_balance AS effective_balance_gwei
    ,slashed
    ,activation_epoch
    ,exit_epoch
    ,withdrawable_epoch
    ,slot AS last_slot
FROM {{ ref('stg_consensus__validators_all') }}
WHERE
    slot_timestamp < today()
    {% if start_month and end_month %}
    AND toStartOfMonth(slot_timestamp) >= toDate('{{ start_month }}')
    AND toStartOfMonth(slot_timestamp) <= toDate('{{ end_month }}')
    {% else %}
    {{ apply_monthly_incremental_filter('slot_timestamp', 'date', 'true', lookback_days=1, filters_sql=range_sql) }}
    {% endif %}
    {% if validator_index_start is not none and validator_index_end is not none %}
    AND validator_index >= {{ validator_index_start }}
    AND validator_index < {{ validator_index_end }}
    {% endif %}
