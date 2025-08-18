SELECT
    date
    ,'withdrawls' AS label
    ,cnt
    ,total_amount
FROM 
    `dbt`.`int_consensus_withdrawls_dist_daily`

UNION ALL

SELECT
    date
    ,'deposits' AS label
    ,cnt
    ,total_amount
FROM 
    `dbt`.`int_consensus_deposits_daily`