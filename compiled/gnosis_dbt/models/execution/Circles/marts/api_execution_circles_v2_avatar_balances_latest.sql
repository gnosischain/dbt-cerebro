

SELECT sub.*, (SELECT toDate(max(date)) FROM `dbt`.`fct_execution_circles_v2_avatar_balances_daily`) AS as_of_date
FROM (
SELECT
    avatar,
    token_address,
    is_wrapped,
    balance,
    balance_demurraged
FROM `dbt`.`fct_execution_circles_v2_avatar_balances_latest`
) AS sub