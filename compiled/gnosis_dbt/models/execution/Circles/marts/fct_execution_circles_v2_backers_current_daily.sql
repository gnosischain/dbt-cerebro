

-- Daily count of CURRENTLY-TRUSTED backers: addresses whose trust interval from
-- the backers group avatar var('circles_target_group_address') is open as of the
-- END of each day. Revocation-aware, so it differs from
-- fct_execution_circles_v2_backers_cumulative_daily, which cumulates
-- first_trusted_at and never drops a backer whose trust was later withdrawn.
--
-- "As of end of day D" = a trust interval is open at (D + 1 day) 00:00 UTC.
-- Open intervals in int_execution_circles_v2_trust_pair_ranges carry the
-- 2106-02-07 sentinel as valid_to.
--
-- Dense calendar: one row per day from circles_target_group_start_date through
-- yesterday, so downstream charts have a continuous time axis. Cardinality is
-- tiny (one truster group, <1k trustees), so the per-day cross join is cheap.

WITH ranges AS (
    SELECT
        lower(trustee) AS backer,
        valid_from_agg,
        valid_to_agg
    FROM `dbt`.`int_execution_circles_v2_trust_pair_ranges`
    WHERE lower(truster) = lower('0x1aca75e38263c79d9d4f10df0635cc6fcfe6f026')
),

calendar AS (
    SELECT
        addDays(toDate('2025-04-25'), n) AS date
    FROM (
        SELECT range(toUInt32(dateDiff(
            'day',
            toDate('2025-04-25'),
            yesterday()
        ) + 1)) AS r
    )
    ARRAY JOIN r AS n
)

SELECT
    c.date AS date,
    uniqExactIf(
        r.backer,
        arrayExists(
            (f, t) -> f < toDateTime64(addDays(c.date, 1), 0)
                      AND ifNull(t, toDateTime64('2106-02-07 06:28:15', 0)) >= toDateTime64(addDays(c.date, 1), 0),
            r.valid_from_agg, r.valid_to_agg
        )
    ) AS currently_trusted_backers
FROM calendar c
CROSS JOIN ranges r
GROUP BY c.date
ORDER BY c.date