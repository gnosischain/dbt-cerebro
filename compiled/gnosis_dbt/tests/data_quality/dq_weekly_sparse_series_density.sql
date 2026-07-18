
-- A daily carry-forward model must have a row for every (entity, day) from the
-- entity's first appearance — density < 1.0 means the incremental window dropped a
-- thin series off its frontier and it is accreting permanent gaps.
-- Lesson: global-frontier-carry-forward (Circles s-gCRC/sDAI pool at 5/48 days).
-- Trailing 90 days; entities younger than 14 days skipped (span too short to judge).
SELECT
    pool_address,
    token_address,
    uniqExact(date) AS days_present,
    dateDiff('day', min(date), max(date)) + 1 AS span_days,
    round(uniqExact(date) / (dateDiff('day', min(date), max(date)) + 1), 3) AS density
FROM `dbt`.`int_execution_pools_balancer_v3_daily`
WHERE date >= today() - 90
GROUP BY pool_address, token_address
HAVING span_days >= 14 AND density < 0.95
ORDER BY density ASC