

SELECT
    date
    ,fork
    ,cnt
FROM `dbt`.`int_p2p_discv5_forks_daily`
WHERE label = 'Current Fork' 
ORDER BY date ASC, fork ASC