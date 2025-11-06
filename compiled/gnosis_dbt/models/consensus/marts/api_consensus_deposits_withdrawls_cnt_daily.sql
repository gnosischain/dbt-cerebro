

SELECT
    date
    ,label
    ,cnt AS value
FROM 
    `dbt`.`int_consensus_deposits_withdrawals_daily`
ORDER BY date, label