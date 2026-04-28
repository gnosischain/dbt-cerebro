


WITH latest_date AS (
  SELECT max(date) AS max_date
  FROM `dbt`.`int_execution_gpay_balances_daily`
  WHERE date < today()
),
agg AS (
  SELECT
    address AS wallet_address,
    symbol AS token,
    sum(round(toFloat64(balance_usd), 2)) AS value_usd,
    sum(round(toFloat64(balance), 6)) AS value_native,
    max(date) AS as_of_date
  FROM `dbt`.`int_execution_gpay_balances_daily`
  WHERE date = (SELECT max_date FROM latest_date)
  GROUP BY wallet_address, token
)

SELECT
  wallet_address,
  token,
  value_usd,
  value_native,
  as_of_date AS date
FROM agg