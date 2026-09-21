{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}
{% set incr_end    = var('incremental_end_date', none) %}

{#
  Was materialized='table': it rebuilt six years of claims from the full census priced
  model every night. That input grew from ~390M to ~426M rows when the 2026-09 census
  repair recovered WxDAI's missing days, and the rebuild then exceeded the server's
  10.8 GiB ceiling -- ClickHouse code 241 on three consecutive nightly crons
  (2026-09-19/20/21), retries included, despite the spill settings below.

  Each day is computed independently from that day's balances -- no carry-forward, no
  {{ this }} -- so the model is naturally incremental. insert_overwrite on the existing
  monthly partition rebuilds the current month (~7M input rows) instead of all history.
  The window comes from apply_monthly_incremental_filter, which is strategy-aware: under
  insert_overwrite it returns WHOLE months, because REPLACE PARTITION would otherwise
  delete the rest of the month a narrower window landed in.

  unique_key is dropped deliberately: insert_overwrite replaces whole partitions and must
  never be combined with a unique_key (see docs/lessons/staged-insert-overwrite-wipe.md).
  Dedup is still handled by the ReplacingMergeTree order_by.
#}

{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if (start_month or incr_end) else 'insert_overwrite'),
        engine='ReplacingMergeTree()',
        order_by='(date, container_address, ubo_address, token_address)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        pre_hook=[
            "SET max_bytes_before_external_group_by = 2000000000",
            "SET max_bytes_before_external_sort = 2000000000",
            "SET join_algorithm = 'grace_hash'"
        ],
        post_hook=[
            "SET max_bytes_before_external_group_by = 0",
            "SET max_bytes_before_external_sort = 0",
            "SET join_algorithm = 'default'"
        ],
        tags=['production','execution','ubo','claims','sdai']
    )
}}

{% set sdai_address = '0xaf204776c7245bf4147c2612bf6e5972ee483701' %}

WITH

-- ─── sDAI HOLDER BALANCES PER DAY ─────────────────────────────────────────────
-- int_execution_tokens_balances_daily already tracks cumulative sDAI holder
-- balances; no Transfer event parsing needed.
sdai_holders AS (
    SELECT
        date,
        lower(address) AS holder,
        balance        AS sdai_balance
    FROM {{ ref('int_rpc_state_indexer_token_balances_priced_daily') }}
    WHERE lower(token_address) = lower('{{ sdai_address }}')
      AND balance > 0
      AND lower(address) != lower('{{ sdai_address }}')
      AND date < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(date) >= toDate('{{ start_month }}')
        AND toStartOfMonth(date) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('date', 'date', 'true') }}
      {% endif %}
),

total_sdai_supply AS (
    SELECT date, sum(sdai_balance) AS total_sdai
    FROM sdai_holders
    GROUP BY date
),

-- ─── WxDAI RESERVE HELD BY THE VAULT ─────────────────────────────────────────
wxdai_reserve AS (
    SELECT
        date,
        lower(token_address) AS token_address,
        symbol,
        token_class,
        balance_raw          AS reserve_raw,
        balance              AS reserve,
        balance_usd          AS reserve_usd
    FROM {{ ref('int_rpc_state_indexer_token_balances_priced_daily') }}
    WHERE lower(address) = lower('{{ sdai_address }}')
      AND symbol = 'WxDAI'
      AND balance > 0
      AND date < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(date) >= toDate('{{ start_month }}')
        AND toStartOfMonth(date) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('date', 'date', 'true') }}
      {% endif %}
)

-- ─── PROPORTIONAL WxDAI CLAIMS ────────────────────────────────────────────────
-- Each sDAI holder's claim on WxDAI = (sDAI_balance / total_sDAI_supply) × vault_WxDAI.
-- Note: aGnosDAI (0x7a5c38...) appears here as a second-level container — its
-- share is attributed to the aToken address rather than individual Aave lenders.
SELECT
    sh.date                                                                              AS date,
    'sDAI'                                                                               AS protocol,
    lower('{{ sdai_address }}')                                                          AS container_address,
    wr.token_address                                                                     AS token_address,
    wr.symbol                                                                            AS symbol,
    wr.token_class                                                                       AS token_class,
    lower(sh.holder)                                                                     AS ubo_address,
    toInt256(round(
        sh.sdai_balance / nullIf(ts.total_sdai, 0) * toFloat64(wr.reserve_raw)
    ))                                                                                   AS balance_raw,
    sh.sdai_balance / nullIf(ts.total_sdai, 0) * wr.reserve                             AS balance,
    sh.sdai_balance / nullIf(ts.total_sdai, 0) * wr.reserve_usd                         AS balance_usd
FROM sdai_holders sh
INNER JOIN total_sdai_supply ts ON ts.date = sh.date
INNER JOIN wxdai_reserve wr     ON wr.date = sh.date
