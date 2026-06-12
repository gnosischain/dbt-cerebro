

SELECT sub.*, (SELECT toDate(max(date)) FROM `dbt`.`int_execution_circles_v2_balances_daily`) AS as_of_date
FROM (
SELECT
    avatar,
    tokens_held_count
FROM `dbt`.`fct_execution_circles_v2_avatar_tokens_held_count`
) AS sub