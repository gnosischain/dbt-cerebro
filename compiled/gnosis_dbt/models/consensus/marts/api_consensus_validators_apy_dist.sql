SELECT
    date,
    q05,
    q10,
    q25,
    q50,
    q75,
    q90,
    q95 
FROM `dbt`.`int_consensus_validators_apy_dist`
ORDER BY date ASC