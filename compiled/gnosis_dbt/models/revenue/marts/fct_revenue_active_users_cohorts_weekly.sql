

WITH per_user AS (
    SELECT
        week,
        user,
        sum(annual_rolling_fees) AS annual_rolling_fees
    FROM `dbt`.`int_revenue_fees_weekly_per_user`
    GROUP BY week, user
)

SELECT
    week,
    
    multiIf(
        
        annual_rolling_fees < 1,                       '<1',
        
        annual_rolling_fees < 3,                        '1-3',
        annual_rolling_fees < 6,                        '3-6',
        annual_rolling_fees < 10,                       '6-10',
        annual_rolling_fees < 100,                      '10-100',
        '>=100'
    )
 AS cohort,
    round(sum(annual_rolling_fees), 2) AS annual_rolling_fees_total,
    countIf(annual_rolling_fees > 0)   AS users_cnt
FROM per_user
WHERE annual_rolling_fees > 0
GROUP BY week, cohort