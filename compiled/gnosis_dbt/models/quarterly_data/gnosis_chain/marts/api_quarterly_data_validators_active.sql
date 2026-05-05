

SELECT
    toStartOfQuarter(date) AS quarter,
    argMax(cnt, date) AS validators_active
FROM `dbt`.`int_consensus_validators_status_daily`
WHERE status = 'active_ongoing'
GROUP BY quarter
ORDER BY quarter