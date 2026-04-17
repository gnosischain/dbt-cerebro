

SELECT
    avatar,
    token_address,
    is_wrapped,
    balance,
    balance_demurraged
FROM `dbt`.`fct_execution_circles_v2_avatar_balances_latest`