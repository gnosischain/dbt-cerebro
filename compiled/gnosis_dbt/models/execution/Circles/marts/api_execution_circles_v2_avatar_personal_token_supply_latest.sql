

SELECT sub.*, (SELECT toDate(max(date)) FROM `dbt`.`int_execution_circles_v2_balances_daily`) AS as_of_date
FROM (
SELECT
    avatar,
    supply,
    wrapped,
    unwrapped,
    wrapped_pct,
    supply_demurraged,
    wrapped_demurraged
FROM `dbt`.`fct_execution_circles_v2_avatar_personal_token_supply_latest`
) AS sub