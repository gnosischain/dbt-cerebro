

SELECT sub.*, (SELECT toDate(max(date)) FROM `dbt`.`int_execution_lending_aave_user_balances_daily`) AS as_of_date
FROM (
SELECT *
FROM `dbt`.`fct_execution_yields_user_lending_positions_latest`
) AS sub