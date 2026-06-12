

SELECT
    date,
    protocol            AS label,
    swap_count          AS value
FROM `dbt`.`fct_execution_trades_by_protocol_daily`
WHERE date < today()
ORDER BY date, label