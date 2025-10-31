{{ config(materialized='view', tags=['production','execution','transactions']) }}

SELECT
  week AS date,
  sector AS label,
  txs AS value
FROM {{ ref('fct_execution_transactions_by_sector_weekly') }}
ORDER BY date ASC, label ASC