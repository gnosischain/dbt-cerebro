-- Freshness monitor for the raw consensus.validators SharedReplacingMergeTree.
--
-- The raw source occasionally contains unmerged duplicate snapshots for the same
-- (slot, validator_index) when the crawler retries an insert (observed: 2025-12-04,
-- 2025-12-12 through 2025-12-16, with ratios up to 2.9x). Downstream dbt models are
-- protected because every stg_consensus__* view uses FINAL, so the dup rows are
-- deduplicated at read time. This test exists to catch the condition before it grows
-- large enough to slow staging queries materially, and to surface it to the ops
-- channel so the crawler team can diagnose the retry path or schedule an
-- OPTIMIZE TABLE ... FINAL DEDUPLICATE pass.
--
-- Returns offending days; passing = zero rows. Looks back 14 days by default to catch
-- a backlog without scanning the whole history.
SELECT
    toDate(slot_timestamp) AS d
    ,count() AS rows_cnt
    ,uniqExact((slot, validator_index)) AS unique_keys
    ,count() - uniqExact((slot, validator_index)) AS dup_rows
    ,(count() - uniqExact((slot, validator_index))) / uniqExact((slot, validator_index)) AS dup_ratio
FROM `consensus`.`validators`
WHERE slot_timestamp >= today() - 14
GROUP BY d
HAVING dup_rows > 0