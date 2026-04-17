{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date)',
        unique_key='(date)',
        partition_by='toStartOfMonth(date)',
        tags=['production','execution','yields','savings_xdai','rate']
    )
}}

-- Canonical Savings xDAI vault rate model.
--
-- Reconstructs share price from ERC4626 Deposit and Withdraw events on the Gnosis Savings
-- xDAI vault (0xaf20...3701) and derives a daily rate that is robust to the vault's
-- lump-sum yield distribution.
--
-- Why not a simple day-over-day ratio: on Gnosis, vault yield is NOT paid continuously
-- per block. The xDAI bridge on Mainnet accrues DSR interest in the sDAI vault there,
-- then periodically (every 1-2 days per forum docs) calls `relayInterest()` which pushes
-- the accumulated xDAI/WXDAI through the InterestReceiver contract. The receiver in turn
-- calls `payInterest()` which deposits the batch into the Gnosis Savings xDAI vault,
-- stepping up its `totalAssets()` (and thus share_price) in a single tx. Between relays
-- the share_price observed at Deposit/Withdraw events is effectively flat; on a relay day
-- it jumps. A naive same-day ratio turns the ~7-day launch warmup (single 1.3% jump on
-- 2023-10-05) into a headline "11,000% APY" spike, even though the real yield over that
-- period was ~60% annualized.
--
-- Fix: compute the daily_rate as the GEOMETRIC SLOPE of share_price over a 7-day window:
--
--     daily_rate = (share_price_today / share_price_7d_ago) ** (1/7) - 1
--
-- This naturally spreads a single lump-sum jump over the preceding week, matching the
-- accrual window the protocol operates on. At steady state (no lumpiness) it equals the
-- true per-day rate. Downstream `fct_yields_savings_xdai_apy_daily` annualizes this to
-- APY and layers on 7DMA / 30DMA / 7DMM / 30DMM windows for further smoothing.
--
-- Regime columns (canonical_label, legacy_symbol, backing_asset, yield_source) are joined
-- from savings_xdai_regimes — the 2025-11-07 bridge upgrade flipped backing from DAI/sDAI
-- to USDS/sUSDS at the same vault address.

{% set window_days = 7 %}

WITH

vault_exchange_events AS (
    SELECT
        block_timestamp,
        log_index,
        toFloat64(toUInt256OrNull(decoded_params['assets']))
          / nullIf(toFloat64(toUInt256OrNull(decoded_params['shares'])), 0) AS share_price
    FROM {{ ref('contracts_sdai_events') }}
    WHERE event_name IN ('Deposit', 'Withdraw')
      AND decoded_params['assets'] IS NOT NULL
      AND decoded_params['shares'] IS NOT NULL
      AND toUInt256OrNull(decoded_params['shares']) != 0
      AND block_timestamp < today()
      {{ apply_monthly_incremental_filter('block_timestamp','date','true') }}
),

share_price_eod AS (
    SELECT
        toDate(block_timestamp) AS date,
        argMax(share_price, (block_timestamp, log_index)) AS share_price
    FROM vault_exchange_events
    GROUP BY date
),

calendar AS (
    SELECT day AS date
    FROM {{ ref('dim_time_spine_daily') }}
    WHERE day >= toDate('2023-09-28')
      AND day <  today()
),

-- Forward-fill share_price across event-less days so every calendar day has a price.
filled AS (
    SELECT
        c.date,
        last_value(s.share_price) IGNORE NULLS OVER (
            ORDER BY c.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS share_price
    FROM calendar c
    LEFT JOIN share_price_eod s ON s.date = c.date
),

-- Rolling 7-day geometric slope. nthValue gives us the price exactly N days ago.
rated AS (
    SELECT
        date,
        share_price,
        first_value(share_price) OVER (
            ORDER BY date
            ROWS BETWEEN {{ window_days }} PRECEDING AND {{ window_days }} PRECEDING
        ) AS window_start_price,
        row_number() OVER (ORDER BY date) AS day_idx
    FROM filled
    WHERE share_price IS NOT NULL
),

rated_with_rate AS (
    SELECT
        date,
        share_price,
        CASE
            -- Need enough history to form the window; and a valid non-zero baseline.
            WHEN day_idx <= {{ window_days }}       THEN NULL
            WHEN window_start_price IS NULL
              OR window_start_price = 0             THEN NULL
            ELSE pow(share_price / window_start_price, 1.0 / {{ window_days }}) - 1
        END AS daily_rate
    FROM rated
),

-- Regime lookup: LEFT JOIN ON requires equality conditions in ClickHouse; pre-compute
-- each date's regime via argMax over the regimes seed.
regime_lookup AS (
    SELECT
        r_outer.date,
        argMax(r.canonical_label, parseDateTimeBestEffort(r.start_ts_utc)) AS canonical_label,
        argMax(r.legacy_symbol,   parseDateTimeBestEffort(r.start_ts_utc)) AS legacy_symbol,
        argMax(r.backing_asset,   parseDateTimeBestEffort(r.start_ts_utc)) AS backing_asset,
        argMax(r.yield_source,    parseDateTimeBestEffort(r.start_ts_utc)) AS yield_source
    FROM rated_with_rate r_outer
    CROSS JOIN {{ ref('savings_xdai_regimes') }} r
    WHERE parseDateTimeBestEffort(r.start_ts_utc) <= toDateTime(r_outer.date)
      AND (r.end_ts_utc = '' OR toDateTime(r_outer.date) < parseDateTimeBestEffort(r.end_ts_utc))
    GROUP BY r_outer.date
)

SELECT
    r.date,
    r.share_price,
    r.daily_rate,
    rl.canonical_label,
    rl.legacy_symbol,
    rl.backing_asset,
    rl.yield_source
FROM rated_with_rate r
LEFT JOIN regime_lookup rl ON rl.date = r.date
WHERE r.daily_rate IS NOT NULL
