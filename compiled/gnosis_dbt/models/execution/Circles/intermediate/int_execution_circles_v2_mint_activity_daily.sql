

-- Densified per-(avatar, date) personal-mint activity with 14-day rolling
-- window aggregates used by the Active Minters KPI and the cohort distribution.
--
-- Replaces Dune's gnosis_circlesv2_mint_activity_v (query_6317871).
--
-- Source: the existing daily mint aggregation view
-- `api_execution_circles_v2_avatar_mint_activity_daily` (one row per
-- avatar × day with `mint_events` and `amount_minted`, derived from
-- int_execution_circles_v2_hub_transfers). We just densify the calendar
-- and apply the 14-day window — no second pass over hub_transfers.
--
-- Window semantics (matches Dune):
--   mint_14dw      = SUM(mint_amount) over the 14 trailing days (inclusive)
--   mint_days_14dw = COUNT(*)         over the 14 trailing days (inclusive)
--
-- Three run modes:
--   * full-refresh / first build  → emit full per-avatar history.
--   * incremental daily run       → emit the last 14 days for affected
--                                   avatars; pull 13 extra preceding rows
--                                   so the window function has a complete
--                                   prefix; delete+insert keyed on
--                                   (avatar, date) keeps other rows intact.
--   * var('start_month'/'end_month') → manual backfill window (matches
--                                      int_execution_gnosis_app_user_activity_daily).




WITH
-- Lookback window over the source daily aggregation. The macro is a no-op
-- on full-refresh (is_incremental() is false → emits nothing → reads all).
mint_events_window AS (
    SELECT
        avatar,
        date,
        amount_minted AS mint_amount
    FROM `dbt`.`api_execution_circles_v2_avatar_mint_activity_daily`
    WHERE 1=1
      
        
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -26))
        FROM `dbt`.`int_execution_circles_v2_mint_activity_daily` AS x1
        WHERE 1=1 
      )
      AND toDate(date) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -26)
          

        FROM `dbt`.`int_execution_circles_v2_mint_activity_daily` AS x2
        WHERE 1=1 
      )
    
  

      
),

affected_avatars AS (
    SELECT DISTINCT avatar FROM mint_events_window
    
    -- Mirror the balances-daily `prev_balances ∪ deltas` pattern: any avatar
    -- already present in `dbt`.`int_execution_circles_v2_mint_activity_daily` within the 14-day rolling window must be
    -- carried forward so its zero-mint rows for the new days get densified
    -- and `mint_days_14dw` / `mint_14dw` continue to roll. Without this,
    -- avatars whose latest mint event predates the incremental boundary are
    -- silently dropped from recent days, collapsing the `<1%` cohort.
    UNION DISTINCT
    SELECT DISTINCT avatar
    FROM `dbt`.`int_execution_circles_v2_mint_activity_daily`
    WHERE date >= addDays(today(), -14)
    
),


-- Incremental: pull 13 extra preceding rows from the persisted table so
-- the rolling window has a complete prefix.
prefix_events AS (
    SELECT avatar, date, mint_amount
    FROM `dbt`.`int_execution_circles_v2_mint_activity_daily`
    WHERE avatar IN (SELECT avatar FROM affected_avatars)
      AND date >= addDays(today(), -40)
      AND date <  (
          SELECT min(date) FROM mint_events_window
      )
),

-- Earliest stored date per affected avatar — anchors the densified calendar
-- so we don't re-emit pre-existing rows.
calendar_lo AS (
    SELECT
        avatar,
        coalesce(min(date), addDays(today(), -27)) AS lo
    FROM `dbt`.`int_execution_circles_v2_mint_activity_daily`
    WHERE avatar IN (SELECT avatar FROM affected_avatars)
      AND date >= addDays(today(), -40)
    GROUP BY avatar
),

mint_events AS (
    SELECT avatar, date, mint_amount FROM mint_events_window
    UNION ALL
    SELECT avatar, date, mint_amount FROM prefix_events
),


calendar_bounds AS (
    SELECT
        avatar,
        
            greatest(lo, addDays(today(), -27))                   AS lo,
            today()                                               AS hi
        
    FROM calendar_lo
),

calendar AS (
    SELECT
        b.avatar,
        addDays(b.lo, n) AS date
    FROM calendar_bounds b
    ARRAY JOIN range(toUInt32(dateDiff('day', b.lo, b.hi) + 1)) AS n
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

-- On incremental, drop the 13-day prefix we pulled in only to seed the
-- window; those (avatar, date) rows already exist in the table and would
-- be unchanged.
WHERE date >= addDays(today(), -14)
