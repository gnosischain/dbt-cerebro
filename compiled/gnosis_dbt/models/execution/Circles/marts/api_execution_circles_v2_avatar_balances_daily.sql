

SELECT
    account AS avatar,
    date,
    token_address,
    toFloat64(balance_raw) / pow(10, 18) AS balance,
    toFloat64(demurraged_balance_raw) / pow(10, 18) AS balance_demurraged
FROM `dbt`.`int_execution_circles_v2_balances_daily`
WHERE date < today() AND balance_raw > POW(10, 15)