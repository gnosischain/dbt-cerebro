{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(domain, date, token_symbol)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','celo','gpay','native','reconciliation']
  )
}}

-- Side-by-side comparison of the Dune-fed pipeline (crawlers_data.*) and the
-- native celo_execution pipeline, restricted to the block range the native
-- indexer has actually covered — comparing outside that range would only
-- measure the backfill gap, not pipeline correctness.
--
-- Two domains, one row per (domain, date, token):
--   transfers : row counts + token volume, Dune vs native
--   wallets   : issued_at counts, Dune vs native (token_symbol = 'ALL')
--
-- Read this table directly when deciding whether to repoint
-- int_celo_gpay_activity / int_celo_gpay_wallets at the native twins: the
-- swap is safe when diff columns hold at ~0 across the covered range for a
-- sustained period.
--
-- CAVEAT while the backfill is partial: the native safe registry only knows
-- Safes whose EnabledModule block has been indexed, so transfers of cards
-- issued in a not-yet-indexed block are invisible to the native side even
-- inside the compared span. Until celo_execution covers the full GP era
-- (June 2026 onward), negative rows_diff mostly measures that registry gap,
-- not decode correctness.

WITH native_span AS (
    -- The native transfer model's own coverage defines the comparable window.
    SELECT
        min(block_time) AS span_start,
        max(block_time) AS span_end
    FROM {{ ref('int_celo_gpay_transfers_native') }}
),

dune_transfers AS (
    SELECT
        block_date              AS date,
        token_symbol,
        count()                 AS dune_rows,
        sum(amount)             AS dune_amount
    FROM {{ source('crawlers_data', 'celo_gpay_transfers') }} FINAL
    WHERE block_time >= (SELECT span_start FROM native_span)
      AND block_time <= (SELECT span_end   FROM native_span)
    GROUP BY date, token_symbol
),

native_transfers AS (
    SELECT
        block_date              AS date,
        token_symbol,
        count()                 AS native_rows,
        sum(amount)             AS native_amount
    FROM {{ ref('int_celo_gpay_transfers_native') }}
    GROUP BY date, token_symbol
),

transfers_compared AS (
    SELECT
        'transfers'                                    AS domain,
        coalesce(d.date, n.date)                       AS date,
        coalesce(nullIf(d.token_symbol, ''), n.token_symbol) AS token_symbol,
        d.dune_rows,
        n.native_rows,
        toInt64(n.native_rows) - toInt64(d.dune_rows)  AS rows_diff,
        round(d.dune_amount, 6)                        AS dune_amount,
        round(n.native_amount, 6)                      AS native_amount,
        round(n.native_amount - d.dune_amount, 6)      AS amount_diff
    FROM dune_transfers d
    FULL OUTER JOIN native_transfers n
        ON d.date = n.date AND d.token_symbol = n.token_symbol
),

dune_wallets AS (
    SELECT
        toDate(event_time) AS date,
        uniqExact(safe_address) AS dune_rows
    FROM {{ source('crawlers_data', 'celo_gpay_wallet_events') }} FINAL
    WHERE action = 'issued_at'
      AND event_time >= (SELECT span_start FROM native_span)
      AND event_time <= (SELECT span_end   FROM native_span)
    GROUP BY date
),

native_wallets AS (
    SELECT
        toDate(event_time) AS date,
        uniqExact(safe_address) AS native_rows
    FROM {{ ref('int_celo_gpay_wallet_events_native') }}
    WHERE action = 'issued_at'
    GROUP BY date
),

wallets_compared AS (
    SELECT
        'wallets'                                     AS domain,
        coalesce(d.date, n.date)                      AS date,
        'ALL'                                         AS token_symbol,
        d.dune_rows,
        n.native_rows,
        toInt64(n.native_rows) - toInt64(d.dune_rows) AS rows_diff,
        CAST(NULL AS Nullable(Float64))               AS dune_amount,
        CAST(NULL AS Nullable(Float64))               AS native_amount,
        CAST(NULL AS Nullable(Float64))               AS amount_diff
    FROM dune_wallets d
    FULL OUTER JOIN native_wallets n
        ON d.date = n.date
)

SELECT * FROM transfers_compared
UNION ALL
SELECT * FROM wallets_compared
