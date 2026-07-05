

-- Quarterly PEAK of blacklist-excluded Active Minters.
-- The +80% bucket (cohort_order = 6) of fct_execution_circles_v2_minter_cohort_daily
-- already excludes the bot/sybil blacklist (stg_crawlers_data__circles_blacklisted)
-- and is the blacklist-excluded equivalent of the Active Minters KPI. The
-- quarterly datapoint is the PEAK daily count within the quarter, matching the
-- Dune circles-v2-kpis "active minters (peak)" presentation. The peak is used
-- (not end-of-quarter) because it is the reported statistic and is robust to
-- recent-tail data settling.

SELECT
    toStartOfQuarter(date) AS quarter,
    max(cnt)               AS active_minters_peak
FROM `dbt`.`fct_execution_circles_v2_minter_cohort_daily`
WHERE cohort_order = 6
  AND date < today()
GROUP BY quarter
ORDER BY quarter