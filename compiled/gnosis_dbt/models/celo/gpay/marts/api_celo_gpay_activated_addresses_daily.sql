

-- Cards that have ever SPENT, cumulative. This is the series
-- api_celo_gpay_funded_addresses_daily served until 2026-08-05, when that model was
-- corrected to mean actually-funded; exposed here under its true name so the metric
-- is not lost. Strictly <= the funded series.
SELECT
    date                 AS date,
    cumulative_activated AS value
FROM `dbt`.`fct_celo_gpay_activity_daily`
ORDER BY date