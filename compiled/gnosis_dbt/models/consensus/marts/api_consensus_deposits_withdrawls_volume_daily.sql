SELECT
    date
    ,label
    ,total_amount AS value
FROM 
    `dbt`.`fct_consensus_deposits_withdrawls_daily`
ORDER BY date, label