

SELECT
    date,
    protocol            AS label,
    volume_usd          AS value
FROM `dbt`.`fct_execution_trades_by_protocol_daily`
WHERE date < today()
ORDER BY date, label