{{
    config(
        materialized='table',
        engine='ReplacingMergeTree()',
        order_by='(date)',
        unique_key='(date)',
        partition_by='toStartOfMonth(date)',
        tags=['production','execution','yields','ocsdai','rate']
    )
}}

-- OpenCover OC-sDAI ("Covered Savings xDAI") ERC-4626 vault share price.
--
-- share_price = assets / shares (sDAI per OC-sDAI share), reconstructed from the
-- vault's ERC-4626 Deposit/Withdraw events (decoded in contracts_ocsdai_events)
-- and forward-filled across event-less days. Both assets (sDAI) and shares
-- (OC-sDAI) are 18-decimal, so the raw ratio is already the normalised
-- "sDAI tokens per OC-sDAI share". Used by int_revenue_ocsdai_user_balances_daily
-- to value OC-sDAI holders' underlying sDAI for the revenue sDAI stream.
--
-- Mirrors int_yields_savings_xdai_rate_daily but only emits the share price:
-- the revenue look-through needs the level (assets-per-share), not a rate/APY.
-- This is a *covered* vault — premiums are streamed out (PremiumStreamed) and
-- deposits/redeems settle asynchronously, so share_price is only directly
-- observed at Deposit/Withdraw events and held flat between them by the
-- forward-fill (the same lumpiness handling the sDAI vault model uses).
--
-- Vault went live 2026-03-16; the calendar floor matches.

WITH vault_exchange_events AS (
    SELECT
        block_timestamp,
        log_index,
        toFloat64(toUInt256OrNull(decoded_params['assets']))
          / nullIf(toFloat64(toUInt256OrNull(decoded_params['shares'])), 0) AS share_price
    FROM {{ ref('contracts_ocsdai_events') }}
    WHERE event_name IN ('Deposit', 'Withdraw')
      AND decoded_params['assets'] IS NOT NULL
      AND decoded_params['shares'] IS NOT NULL
      AND toUInt256OrNull(decoded_params['shares']) != 0
      AND block_timestamp < today()
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
    WHERE day >= toDate('2026-03-16')
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
)

SELECT
    date,
    share_price
FROM filled
WHERE share_price IS NOT NULL
