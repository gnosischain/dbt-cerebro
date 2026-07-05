

-- Densified per-(avatar, date) personal-mint activity with a 14-day rolling
-- window, feeding the Active Minters KPI and the minter cohort distribution.
-- Replaces Dune's gnosis_circlesv2_mint_activity_v (query_6317871).
--
-- Materialized as a FULL-REFRESH table (previously incremental).
-- Rationale: the metric is a 14-day TRAILING window whose recent tail must be
-- recomputed over a COMPLETE prefix. An incremental delete+insert cannot
-- guarantee a complete prefix after out-of-order mint backfills, which
-- silently zeroed the recent tail (observed: active_minters = 0 on days with
-- thousands of minters, then a rebuild-signature climb). At ~3.5M rows /
-- ~19k avatars the full rebuild is sub-second, so correctness beats the
-- incremental machinery. The start_month/end_month backfill branch is also
-- dropped (a table-materialized model with month-var branches is truncated by
-- batched refreshes).
--
-- Window semantics (matches Dune):
--   mint_14dw      = SUM(mint_amount) over the 14 trailing days (inclusive)
--   mint_days_14dw = COUNT(*)         over the 14 trailing days (inclusive)
--
-- Source: api_execution_circles_v2_avatar_mint_activity_daily (one row per
-- avatar x mint-day with amount_minted), derived from the v2 hub transfers.

WITH mint_events AS (
    SELECT
        avatar,
        date,
        amount_minted AS mint_amount
    FROM `dbt`.`api_execution_circles_v2_avatar_mint_activity_daily`
),

calendar_lo AS (
    SELECT
        avatar,
        min(date) AS lo
    FROM mint_events
    GROUP BY avatar
),

-- Dense per-avatar calendar from each avatar's first mint through today, so
-- the trailing-14-day window has a row for every day (zero-mint days included)
-- and mint_14dw / mint_days_14dw roll over a gap-free prefix.
calendar AS (
    SELECT
        avatar,
        addDays(lo, n) AS date
    FROM calendar_lo
    ARRAY JOIN range(toUInt32(dateDiff('day', lo, today()) + 1)) AS n
),

dense AS (
    SELECT
        c.avatar,
        c.date,
        coalesce(m.mint_amount, 0) AS mint_amount
    FROM calendar c
    LEFT JOIN mint_events m
        ON m.avatar = c.avatar
       AND m.date   = c.date
),

windowed AS (
    SELECT
        avatar,
        date,
        mint_amount,
        sum(mint_amount) OVER (
            PARTITION BY avatar
            ORDER BY date
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS mint_14dw,
        count() OVER (
            PARTITION BY avatar
            ORDER BY date
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS mint_days_14dw
    FROM dense
)

SELECT
    avatar,
    date,
    mint_amount,
    mint_14dw,
    toUInt8(mint_days_14dw) AS mint_days_14dw
FROM windowed