{{ config(
  materialized='table',
  engine='ReplacingMergeTree()',
  order_by='(week, sector)',
  partition_by='toYYYYMM(week)',
  tags=['production','execution','transactions']
) }}

SELECT
  toStartOfWeek(date)                          AS week,
  sector,
  toUInt64(groupBitmapMerge(ua_bitmap_state))  AS active_accounts,  
  sum(tx_count)                                AS txs,
  sum(gas_used_sum)                            AS gas_used_sum,
  round(toFloat64(sum(fee_native_sum)), 2)     AS fee_native_sum
FROM {{ ref('int_execution_transactions_by_project_daily') }}
WHERE date < toStartOfWeek(today())            
GROUP BY week, sector