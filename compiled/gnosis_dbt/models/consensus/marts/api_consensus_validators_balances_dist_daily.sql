

-- in GNO
SELECT
    date,
    q05/32 AS q05,
    q10/32 AS q10,
    q25/32 AS q25,
    q50/32 AS q50,
    q75/32 AS q75,
    q90/32 AS q90,
    q95/32 AS q95
FROM `dbt`.`int_consensus_validators_balances_dist_daily`
ORDER BY date ASC