

WITH latest AS (
    SELECT max(date) AS d FROM `dbt`.`int_dao_treasury_holdings_daily`
)

SELECT
    wallet_label,
    wallet_address,
    symbol,
    position_type,
    protocol,
    round(balance, 4) AS balance,
    round(balance_usd, 2) AS balance_usd
FROM `dbt`.`int_dao_treasury_holdings_daily`
WHERE date = (SELECT d FROM latest)
  AND balance_usd > 1
ORDER BY balance_usd DESC