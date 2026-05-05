

SELECT
    toStartOfQuarter(date) AS quarter,
    argMax(active_trusts, date) AS active_trusts
FROM `dbt`.`fct_execution_circles_v2_active_trusts_daily`
WHERE date < today()
GROUP BY quarter
ORDER BY quarter