



WITH per_user AS (
    SELECT
        month,
        user,
        sum(month_fees) AS month_fees
    FROM `dbt`.`int_revenue_fees_monthly_per_user`
    GROUP BY month, user
)

SELECT
    month,
    countIf(month_fees >= 0.5) AS users_cnt,
    round(sumIf(month_fees, month_fees >= 0.5), 2) AS fees_total
FROM per_user
GROUP BY month