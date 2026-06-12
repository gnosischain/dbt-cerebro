

SELECT sub.*, (SELECT toDate(max(date)) FROM `dbt`.`int_consensus_validators_income_daily`) AS as_of_date
FROM (
SELECT * FROM `dbt`.`fct_consensus_validators_explorer_members_table`
) AS sub