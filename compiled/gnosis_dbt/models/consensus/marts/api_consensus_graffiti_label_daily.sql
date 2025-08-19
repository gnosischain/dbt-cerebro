SELECT
    date
    ,label
    ,SUM(cnt) AS value
FROM `dbt`.`int_consensus_graffiti_daily`
GROUP BY date, label
ORDER BY date, label