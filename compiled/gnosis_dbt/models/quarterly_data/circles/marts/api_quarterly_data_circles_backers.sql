

-- Quarterly CURRENTLY-TRUSTED backers, as of quarter end (revocation-aware).
-- Takes the currently-trusted count on the latest available day within each
-- quarter (= the quarter's last calendar day for a closed quarter). This is the
-- end-of-quarter snapshot the quarterly report uses. Distinct from the ever-backed
-- cumulative series (api:circles_v2_backers_cumulative), which never drops backers
-- whose trust was later revoked.

SELECT
    toStartOfQuarter(date) AS quarter,
    argMax(currently_trusted_backers, date) AS total_backers
FROM `dbt`.`fct_execution_circles_v2_backers_current_daily`
WHERE date < today()
GROUP BY quarter
ORDER BY quarter