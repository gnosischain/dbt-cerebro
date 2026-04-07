

SELECT
    date,
    total_supply AS value,
    total_demurraged_supply AS value_demurraged,
    token_count
FROM `dbt`.`fct_execution_circles_v2_total_supply_daily`
WHERE date < today()
ORDER BY date DESC