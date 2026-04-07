

SELECT
    avatar,
    day AS date,
    trusts_given_count,
    trusts_received_count
FROM `dbt`.`fct_execution_circles_v2_avatar_trusts_daily`
WHERE avatar IS NOT NULL