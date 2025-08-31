

SELECT 
    date
    ,status
    ,cnt
FROM `dbt`.`int_consensus_validators_status_daily`
WHERE status NOT IN ('active_ongoing', 'withdrawal_done')
ORDER BY date, status