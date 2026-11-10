

-- Until 2026-11 this mart also returned is_estimated (true when the quarter's last day
-- used forward-filled carbon intensity). The ESG chain now waits for Ember's
-- publication, so no quarter is estimated and the column was dropped.

SELECT
    toStartOfQuarter(date) AS quarter,
    argMax(annual_co2_tonnes_projected, date) AS co2_tonnes_yr
FROM `dbt`.`int_quarterly_esg_carbon_footprint_with_fallback`
WHERE toStartOfMonth(date) < toStartOfMonth(today())
GROUP BY quarter
ORDER BY quarter