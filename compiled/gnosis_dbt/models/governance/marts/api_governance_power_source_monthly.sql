

-- Voting power by source (Staked GNO / GNO holdings / Delegated) per month,
-- long format (date, label, value). Populated once proposals are ingested with
-- the `strategies` field (see int_governance_vote_power_source).
SELECT
    toStartOfMonth(proposal_created_at) AS date,
    power_source                        AS label,
    round(sum(strategy_vp), 1)          AS value,
    uniqExact(voter)                    AS voters
FROM `dbt`.`int_governance_vote_power_source`
WHERE proposal_created_at > toDateTime('2020-01-01 00:00:00', 'UTC')
GROUP BY date, power_source
ORDER BY date, label