

SELECT
    date,
    aggregator_label    AS label,
    share_pct           AS value
FROM `dbt`.`fct_execution_trades_by_aggregator_daily`
WHERE date < today()
ORDER BY date, label