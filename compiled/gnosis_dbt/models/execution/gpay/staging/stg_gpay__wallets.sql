

SELECT
    address
    ,MIN(introduced_at) AS introduced_at
FROM `dbt`.`int_crawlers_data_labels`
WHERE project = 'gpay'
GROUP BY address