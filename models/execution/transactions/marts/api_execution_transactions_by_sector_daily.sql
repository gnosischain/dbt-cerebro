{{ config(materialized='view', tags=['production','execution','transactions', 'tier1', 'api: cnt_by_sector_d']) }}

SELECT
  date,
  sector AS label,
  txs AS value
FROM {{ ref('fct_execution_transactions_by_sector_daily') }}
WHERE date < today()
ORDER BY date ASC, label ASC
