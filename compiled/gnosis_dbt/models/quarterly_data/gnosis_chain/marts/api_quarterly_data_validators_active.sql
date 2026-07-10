

-- Active validators = the full active set (active_ongoing + active_exiting) at
-- quarter-end, matching public beacon explorers (dora / beaconcha). Exiting
-- validators still attest and remain staked until their exit completes, so they
-- are part of the active set. The two statuses are summed PER DAY before the
-- quarter-end argMax: a bare argMax(cnt, date) over both status rows would pick
-- one row arbitrarily on the last day rather than their sum.
WITH per_day AS (
    SELECT
        date,
        sum(cnt) AS active_cnt
    FROM `dbt`.`int_consensus_validators_status_daily`
    WHERE status IN ('active_ongoing', 'active_exiting')
    GROUP BY date
)

SELECT
    toStartOfQuarter(date) AS quarter,
    argMax(active_cnt, date) AS validators_active
FROM per_day
GROUP BY quarter
ORDER BY quarter