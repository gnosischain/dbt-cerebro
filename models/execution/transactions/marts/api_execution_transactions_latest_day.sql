{{ config(materialized='view', tags=['production','execution','transactions']) }}

SELECT
  value,
  change_pct
FROM {{ ref('fct_execution_transactions_snapshots') }}
WHERE label = 'Transactions'
  AND window = '1D'