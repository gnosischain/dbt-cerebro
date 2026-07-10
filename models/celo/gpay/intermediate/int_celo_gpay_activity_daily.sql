{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    engine='ReplacingMergeTree()',
    order_by='(date, safe_address, action, token_symbol)',
    partition_by='toStartOfMonth(date)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','execution','celo','gpay','activity_daily']
  )
}}

-- Mirrors int_execution_gpay_activity_daily's own incremental pattern
-- exactly (same macro, same signature) — reuse, not a new invention.
SELECT
    date,
    safe_address,
    action,
    token_symbol,
    token_address,
    SUM(amount)                        AS amount,
    SUM(amount_usd)                    AS amount_usd,
    COUNT()                            AS activity_count
FROM {{ ref('int_celo_gpay_activity') }}
{{ apply_monthly_incremental_filter('date', 'date', false) }}
GROUP BY date, safe_address, action, token_symbol, token_address
