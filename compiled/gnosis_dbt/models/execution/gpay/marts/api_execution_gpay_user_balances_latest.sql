

SELECT sub.*, (SELECT toDate(max(date)) FROM `dbt`.`int_execution_gpay_balances_daily`) AS as_of_date
FROM (
SELECT
  wallet_address,
  token,
  value_usd,
  value_native,
  date
FROM `dbt`.`fct_execution_gpay_user_balances_latest`
) AS sub