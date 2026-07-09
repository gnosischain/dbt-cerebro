{% set start_month = var('start_month', none) %}
{% set end_month = var('end_month', none) %}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        engine='ReplacingMergeTree()',
        order_by='(date)',
        partition_by='toStartOfMonth(date)',
        tags=["production", "consensus", "validators_balances"]
    )
}}


-- Values in real GNO. Source columns are gwei-of-mGNO (32 mGNO = 1 GNO), so the
-- full conversion is /1e9 (gwei) /32 (mGNO -> GNO), applied HERE at the origin —
-- downstream marts must NOT divide again.
-- Full-history rebuilds must go through scripts/full_refresh/refresh.py (see
-- meta.full_refresh in schema.yml): a single-pass FINAL scan over the whole
-- stg_consensus__validators history exceeds the 10.8 GiB memory cap (CH 241).
SELECT
    toStartOfDay(slot_timestamp) AS date
    ,SUM(balance/POWER(10,9)/32) AS balance
    ,SUM(effective_balance/POWER(10,9)/32) AS effective_balance
FROM {{ ref('stg_consensus__validators') }}
WHERE
    slot_timestamp < today()
    {% if start_month and end_month %}
    AND toStartOfMonth(slot_timestamp) >= toDate('{{ start_month }}')
    AND toStartOfMonth(slot_timestamp) <= toDate('{{ end_month }}')
    {% else %}
    {{ apply_monthly_incremental_filter('slot_timestamp', 'date', 'true') }}
    {% endif %}
GROUP BY date