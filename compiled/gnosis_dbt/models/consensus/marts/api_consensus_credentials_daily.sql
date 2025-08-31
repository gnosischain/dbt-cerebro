

SELECT 
    date
    ,credentials_type
    ,cnt
    ,ROUND(cnt/(SUM(cnt) OVER (PARTITION BY date)) * 100,2) AS pct 
FROM `dbt`.`int_consensus_credentials_daily`
ORDER BY date, credentials_type