

SELECT sub.*, (SELECT toDate(max(date)) FROM `dbt`.`int_consensus_credentials_daily`) AS as_of_date
FROM (
SELECT 
    credentials_type
    ,cnt
FROM `dbt`.`int_consensus_credentials_daily`
WHERE date = (SELECT MAX(date) FROM `dbt`.`int_consensus_credentials_daily`)
) AS sub