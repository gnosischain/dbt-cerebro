

-- Daily currently-trusted backers (revocation-aware), latest day excluded.
-- Distinct from api:circles_v2_backers_cumulative, which is the ever-backed
-- (monotonic) cumulative series.

SELECT
    date,
    currently_trusted_backers
FROM `dbt`.`fct_execution_circles_v2_backers_current_daily`
WHERE date < today()
ORDER BY date