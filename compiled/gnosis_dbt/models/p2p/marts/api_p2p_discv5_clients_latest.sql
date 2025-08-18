SELECT
    metric
    ,label
    ,value
FROM `dbt`.`int_p2p_discv5_clients_daily`
WHERE date = (SELECT MAX(date) FROM  `dbt`.`int_p2p_discv5_clients_daily` )
ORDER BY metric, label