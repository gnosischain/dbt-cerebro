

SELECT
    date,
    holder_type AS label,
    supply AS value,
    demurraged_supply AS value_demurraged,
    holder_count
FROM `dbt`.`fct_execution_circles_v2_supply_by_holder_type_daily`
WHERE date < today()
ORDER BY date DESC, label