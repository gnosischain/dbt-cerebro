

-- Daily cohort distribution of Circles v2 minters by 14-day mint coverage.
--
-- For every avatar with `mint_days_14dw = 14` (i.e., minted on each of the
-- last 14 days), bucket by the share of the theoretical 14-day maximum
-- (336 CRC) covered by their actual mint_14dw. Blacklisted avatars are
-- excluded — this mirrors the Dune query that filters
-- is_blacklisted = False before grouping.
--
-- The blacklist join lives here (a small mart, full rebuild) rather than
-- in the intermediate rolling view, so a fresh `crawlers_data.circles_blacklisted`
-- snapshot from click-runner takes effect on the next dbt run without
-- needing to backfill the rolling window.

WITH base AS (
    SELECT
        m.date,
        m.avatar,
        m.mint_14dw
    FROM `dbt`.`int_execution_circles_v2_mint_activity_daily` m
    LEFT JOIN `dbt`.`stg_crawlers_data__circles_blacklisted` b
        ON b.address = m.avatar
    WHERE m.mint_days_14dw = 14
      AND b.address IS NULL
),

bucketed AS (
    SELECT
        date,
        multiIf(
            mint_14dw <  0.01 * 336, '<1%',
            mint_14dw <  0.20 * 336, '[1%, 20%[',
            mint_14dw <  0.40 * 336, '[20%, 40%[',
            mint_14dw <  0.60 * 336, '[40%, 60%[',
            mint_14dw <  0.80 * 336, '[60%, 80%[',
                                     '+80%'
        ) AS cohort,
        multiIf(
            mint_14dw <  0.01 * 336, toUInt8(1),
            mint_14dw <  0.20 * 336, toUInt8(2),
            mint_14dw <  0.40 * 336, toUInt8(3),
            mint_14dw <  0.60 * 336, toUInt8(4),
            mint_14dw <  0.80 * 336, toUInt8(5),
                                     toUInt8(6)
        ) AS cohort_order
    FROM base
)

SELECT
    date,
    cohort_order,
    cohort,
    count() AS cnt
FROM bucketed
GROUP BY date, cohort_order, cohort
ORDER BY date, cohort_order