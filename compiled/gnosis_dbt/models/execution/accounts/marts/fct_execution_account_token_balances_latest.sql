

-- Bound the max(date) scan to a small recent window so ClickHouse only reads
-- a couple of monthly partitions instead of every partition since 2020.
-- Without this, max(date) triggers a full-tree scan that OOMs on the 10.8 GiB
-- cluster cap.
WITH latest_date AS (
  SELECT max(date) AS max_date
  FROM `dbt`.`int_execution_tokens_balances_daily`
  WHERE date >= today() - 14
    AND date < today()
),

latest_balances AS (
  SELECT
    lower(address) AS address,
    date,
    lower(token_address) AS token_address,
    symbol,
    token_class,
    balance_raw,
    balance,
    ifNull(balance_usd, 0) AS balance_usd
  FROM `dbt`.`int_execution_tokens_balances_daily`
  WHERE date >= today() - 14
    AND date = (SELECT max_date FROM latest_date)
    AND address IS NOT NULL
    AND address != ''
    AND balance > 0
)

SELECT
  address,
  date,
  token_address,
  symbol,
  token_class,
  balance_raw,
  balance,
  balance_usd
FROM latest_balances