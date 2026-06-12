

SELECT sub.*, (SELECT toDate(max(block_date)) FROM `dbt`.`int_execution_safes`) AS as_of_date
FROM (
SELECT * FROM `dbt`.`fct_execution_account_profile_latest`
) AS sub