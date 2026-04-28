

SELECT
    date,
    sum(supply_raw) AS total_supply_raw,
    sum(supply) AS total_supply,
    sum(demurraged_supply) AS total_demurraged_supply,
    count() AS token_count
FROM `dbt`.`int_execution_circles_v2_tokens_supply_daily`
GROUP BY date
ORDER BY date