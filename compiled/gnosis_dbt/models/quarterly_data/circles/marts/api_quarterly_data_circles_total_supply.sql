

SELECT
    toStartOfQuarter(date) AS quarter,
    argMax(total_supply, date) AS total_supply,
    argMax(total_demurraged_supply, date) AS total_supply_demurraged
FROM `dbt`.`fct_execution_circles_v2_total_supply_daily`
WHERE date < today()
GROUP BY quarter
ORDER BY quarter