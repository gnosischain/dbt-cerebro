

WITH latest AS (
    SELECT max(day) AS d
    FROM `dbt`.`fct_execution_circles_v2_avatar_trusts_daily`
    WHERE day < today()
)

SELECT
    t.avatar,
    t.trusts_given_count,
    t.trusts_received_count
FROM `dbt`.`fct_execution_circles_v2_avatar_trusts_daily` t
CROSS JOIN latest
WHERE t.day = latest.d AND t.avatar IS NOT NULL