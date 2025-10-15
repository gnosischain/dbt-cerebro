

SELECT
  date,
  sector AS label,
  txs AS value
FROM `dbt`.`fct_execution_transactions_by_sector_daily`
WHERE date < today()
ORDER BY date ASC, label ASC