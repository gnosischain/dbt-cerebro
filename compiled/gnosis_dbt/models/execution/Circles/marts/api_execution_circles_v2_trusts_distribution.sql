

SELECT sub.*, (SELECT toDate(max(day)) FROM `dbt`.`fct_execution_circles_v2_avatar_trusts_daily`) AS as_of_date
FROM (
-- Distribution histogram of trust degree (given / received) across avatars,
-- bucketed (0 / 1-5 / 6-10 / 11-25 / 26-50 / 51-100 / 100+). Thin passthrough
-- over fct_execution_circles_v2_trusts_distribution.

SELECT
    direction,
    trust_bucket,
    avatar_count
FROM `dbt`.`fct_execution_circles_v2_trusts_distribution`
ORDER BY direction, trust_bucket
) AS sub