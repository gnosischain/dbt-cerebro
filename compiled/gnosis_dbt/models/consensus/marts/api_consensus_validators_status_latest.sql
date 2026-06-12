

SELECT sub.*, today() AS as_of_date
FROM (
SELECT
    *
FROM `dbt`.`fct_consensus_validators_status_latest`
) AS sub