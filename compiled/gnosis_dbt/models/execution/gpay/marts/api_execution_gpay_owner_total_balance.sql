

SELECT sub.*, (SELECT toDate(max(date)) FROM `dbt`.`fct_execution_gpay_owner_balances_by_token_daily`) AS as_of_date
FROM (
SELECT
    round(toFloat64(sum(balance_usd)), 2) AS value
FROM `dbt`.`fct_execution_gpay_owner_balances_by_token_daily`
WHERE date = (SELECT max(date) FROM `dbt`.`fct_execution_gpay_owner_balances_by_token_daily`)
  AND symbol IN ('EURe', 'GBPe', 'USDC.e')
) AS sub