{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}
{% set incr_end    = var('incremental_end_date', none) %}

{#
  Daily supply and holder counts per token from the census.

  Two supply definitions are published side by side, deliberately:

    supply_total    the contract's own totalSupply() at the day's anchor, minus whatever
                    sits at 0x0. Old tokens burn by sending to the zero address without
                    decrementing totalSupply, so the raw scalar overstates circulating
                    supply (docs/lessons/old-token-burned-supply-overstatement.md).
    supply_holders  the sum of observed holder balances excluding 0x0 — the definition
                    int_execution_tokens_supply_holders_daily publishes today, kept so the
                    series stays comparable across the cutover.

  Where they disagree the gap is real signal: mints or burns that never emitted a Transfer,
  or a holder universe the discovery scan has not reached. A test on that gap belongs here
  once the history is built and a tolerance has been measured.

  Grain is driven by the scalars, which are a superset of the balances: daily_token_supply
  covers ~3,400 tokens and daily_curated_balances records a totalSupply of its own for each
  of its tokens, so every token-day with balances also has a scalar. supply_holders and
  holders are therefore NULL (not zero) for whitelist tokens that carry supply but are not
  in the curated balances set — hence join_use_nulls, paired with its reset.

  Several jobs record totalSupply for the same token-day and all read the same anchor
  block, so the scalar is deduplicated with max() per token-day rather than filtered to one
  job. Balances are read from the curated job only: mixing jobs there would double-count.
#}

{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if (start_month or incr_end) else 'insert_overwrite'),
    engine='ReplacingMergeTree()',
    order_by='(date, token_address)',
    partition_by='toStartOfMonth(date)',
    settings={ 'allow_nullable_key': 1 },
    pre_hook=["SET join_use_nulls = 1"],
    post_hook=["SET join_use_nulls = 0"],
    tags=['production','rpc_state_indexer','tokens','supply_holders_daily','microbatch']
  )
}}

{% set chain_id     = 100 %}
{% set balances_job = "'daily_curated_balances'" %}
{% set zero_address = "'0x0000000000000000000000000000000000000000'" %}

WITH scalars AS (
    SELECT
        s.snapshot_date AS date,
        s.token_address AS token_address,
        max(toInt256(s.scalar_raw)) AS total_supply_raw
    FROM {{ ref('stg_rpc_state_indexer__token_scalars_published') }} AS s
    WHERE s.chain_id = {{ chain_id }}
      AND s.scalar_name = 'totalSupply'
      AND s.snapshot_date < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(s.snapshot_date) >= toDate('{{ start_month }}')
        AND toStartOfMonth(s.snapshot_date) <= toDate('{{ end_month }}')
      {% elif is_incremental() %}
        AND toStartOfMonth(s.snapshot_date) >= (
            SELECT toStartOfMonth(max(date)) FROM {{ this }}
        )
      {% endif %}
    GROUP BY date, token_address
),

balances AS (
    SELECT
        b.snapshot_date AS date,
        b.token_address AS token_address,
        sumIf(toInt256(b.balance_raw), b.holder_address != {{ zero_address }}) AS held_raw,
        sumIf(toInt256(b.balance_raw), b.holder_address = {{ zero_address }}) AS burned_raw,
        countIf(b.balance_raw > 0 AND b.holder_address != {{ zero_address }}) AS holders
    FROM {{ ref('stg_rpc_state_indexer__token_balances_published') }} AS b
    WHERE b.chain_id = {{ chain_id }}
      AND b.job_name = {{ balances_job }}
      AND b.snapshot_date < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(b.snapshot_date) >= toDate('{{ start_month }}')
        AND toStartOfMonth(b.snapshot_date) <= toDate('{{ end_month }}')
      {% elif is_incremental() %}
        AND toStartOfMonth(b.snapshot_date) >= (
            SELECT toStartOfMonth(max(date)) FROM {{ this }}
        )
      {% endif %}
    GROUP BY date, token_address
)

SELECT
    s.date AS date,
    s.token_address AS token_address,
    w.symbol AS symbol,
    w.token_class AS token_class,
    (s.total_supply_raw - coalesce(b.burned_raw, toInt256(0))) / POWER(10, w.decimals) AS supply_total,
    b.held_raw / POWER(10, w.decimals) AS supply_holders,
    b.holders AS holders
FROM scalars AS s
LEFT JOIN balances AS b
    ON b.date = s.date
   AND b.token_address = s.token_address
INNER JOIN {{ ref('tokens_whitelist') }} AS w
    ON lower(w.address) = s.token_address
   AND s.date >= toDate(w.date_start)
   AND (w.date_end IS NULL OR s.date < toDate(w.date_end))
