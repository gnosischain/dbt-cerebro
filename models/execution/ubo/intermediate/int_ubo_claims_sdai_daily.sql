{{
    config(
        materialized='table',
        engine='ReplacingMergeTree()',
        order_by='(date, container_address, ubo_address, token_address)',
        unique_key='(date, container_address, ubo_address, token_address)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
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
    FROM {{ ref('int_execution_tokens_balances_daily') }}
    WHERE lower(token_address) = lower('{{ sdai_address }}')
      AND balance > 0
      AND lower(address) != lower('{{ sdai_address }}')
      AND date < today()
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
    FROM {{ ref('int_execution_tokens_balances_daily') }}
    WHERE lower(address) = lower('{{ sdai_address }}')
      AND symbol = 'WxDAI'
      AND balance > 0
      AND date < today()
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
