

SELECT
    date
    ,label
    ,total_amount AS value
FROM 
    `dbt`.`int_consensus_deposits_withdrawals_daily`
ORDER BY date, label