{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}
{% set incr_end    = var('incremental_end_date', none) %}

{#
  Daily holder balances read from the rpc-state-indexer census instead of accumulated
  from Transfer logs. The census observes balanceOf() at the day's verified anchor block,
  so every day stands on its own: there is no carry-forward, no running sum, no seed day,
  and therefore no way for one bad day to poison every later one.

  Two consequences worth stating, because they are what makes this model safe where
  int_execution_tokens_balances_native_daily is not:

  1. The incremental filter cuts on WHOLE MONTHS, never on days. insert_overwrite REPLACEs
     a whole partition, so a day-bounded window would delete the rest of the month it
     lands in (2026-09-15: a failed watermark read narrowed the window to 09-09 and took
     09-10..09-13 with it). Selecting whole months makes every partition rewrite complete
     by construction, whatever the watermark says.
  2. No tombstone rows. The transfer-derived model must emit zero-balance rows so a
     delete+insert can overwrite a stale balance; here the month is rewritten wholesale,
     so a holder who spent to zero simply stops appearing.

  Scope is the tokens_whitelist seed (INNER JOIN): the seed stays the single source of
  token identity, decimals and the active date window, and the census is a superset of it.
#}

{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if (start_month or incr_end) else 'insert_overwrite'),
    engine='ReplacingMergeTree()',
    order_by='(date, token_address, address)',
    partition_by='toStartOfMonth(date)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','rpc_state_indexer','tokens','balances_daily','microbatch']
  )
}}

{% set chain_id      = 100 %}
{% set balances_job  = "'daily_curated_balances'" %}

WITH census AS (
    SELECT
        b.snapshot_date AS date,
        b.token_address AS token_address,
        b.holder_address AS address,
        toInt256(b.balance_raw) AS balance_raw
    FROM {{ ref('stg_rpc_state_indexer__token_balances_published') }} AS b
    WHERE b.chain_id = {{ chain_id }}
      AND b.job_name = {{ balances_job }}
      AND b.balance_raw > 0
      AND b.snapshot_date < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(b.snapshot_date) >= toDate('{{ start_month }}')
        AND toStartOfMonth(b.snapshot_date) <= toDate('{{ end_month }}')
      {% elif is_incremental() %}
        AND toStartOfMonth(b.snapshot_date) >= (
            SELECT toStartOfMonth(max(date)) FROM {{ this }}
        )
      {% endif %}
)

SELECT
    c.date AS date,
    c.token_address AS token_address,
    w.symbol AS symbol,
    w.token_class AS token_class,
    c.address AS address,
    c.balance_raw AS balance_raw,
    c.balance_raw / POWER(10, w.decimals) AS balance
FROM census AS c
INNER JOIN {{ ref('tokens_whitelist') }} AS w
    ON lower(w.address) = c.token_address
   AND c.date >= toDate(w.date_start)
   AND (w.date_end IS NULL OR c.date < toDate(w.date_end))
