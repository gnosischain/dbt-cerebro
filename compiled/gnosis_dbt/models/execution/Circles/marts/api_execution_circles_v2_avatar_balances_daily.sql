

SELECT
    avatar,
    date,
    token_address,
    balance,
    balance_demurraged
FROM `dbt`.`fct_execution_circles_v2_avatar_balances_daily`
WHERE date < today()