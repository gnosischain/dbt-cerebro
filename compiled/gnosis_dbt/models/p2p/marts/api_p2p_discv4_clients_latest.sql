

SELECT sub.*, (SELECT toDate(max(date)) FROM `dbt`.`int_p2p_discv4_clients_daily`) AS as_of_date
FROM (
SELECT
    metric
    ,label
    ,value
FROM `dbt`.`int_p2p_discv4_clients_daily`
WHERE date = (SELECT MAX(date) FROM  `dbt`.`int_p2p_discv4_clients_daily` )
ORDER BY metric, label
) AS sub