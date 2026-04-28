

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
    
    multiIf(
        month_fees < 0.01,                     '<0.01',
        month_fees < 0.1,                      '0.01-0.1',
        month_fees < 0.5,                      '0.1-0.5',
        month_fees < 1,                        '0.5-1',
        month_fees < 3,                        '1-3',
        month_fees < 6,                        '3-6',
        month_fees < 10,                       '6-10',
        month_fees < 100,                      '10-100',
        '>=100'
    )
 AS cohort,
    round(sum(month_fees), 2) AS fees_total,
    countIf(month_fees > 0)   AS users_cnt
FROM per_user
WHERE month_fees >= 0.01
GROUP BY month, cohort