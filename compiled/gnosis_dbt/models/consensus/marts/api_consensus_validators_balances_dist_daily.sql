

-- in GNO
SELECT
    date,
    q05_balance/32 AS q05,
    q10_balance/32 AS q10,
    q25_balance/32 AS q25,
    q50_balance/32 AS q50,
    q75_balance/32 AS q75,
    q90_balance/32 AS q90,
    q95_balance/32 AS q95
FROM `dbt`.`int_consensus_validators_dists_daily`
ORDER BY date ASC