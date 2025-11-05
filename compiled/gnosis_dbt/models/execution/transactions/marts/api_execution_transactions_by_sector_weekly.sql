

SELECT
  week AS date,
  sector AS label,
  txs AS value
FROM `dbt`.`fct_execution_transactions_by_sector_weekly`
ORDER BY date ASC, label ASC