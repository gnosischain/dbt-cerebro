

SELECT
    date,
    user_address,
    reserve_address,
    symbol,
    round(balance, 6)      AS balance,
    round(balance_usd, 2)  AS balance_usd
FROM `dbt`.`int_execution_lending_aave_user_balances_daily`
WHERE balance_usd > 0.01