{{ config(
  materialized='table',
  engine='ReplacingMergeTree()',
  order_by='(date, label, metric)',
  partition_by='toStartOfYear(date)',
  unique_key='(date, label, metric)',
  tags=['production','execution','transactions']
) }}

WITH base AS (
  SELECT
    toStartOfMonth(date)                    AS month,
    project,
    sum(tx_count)                           AS txs,
    sum(fee_native_sum)                     AS fee_native,
    sum(gas_used_sum)                       AS gas_used,
    groupBitmapMergeState(ua_bitmap_state)  AS active_state
  FROM {{ ref('int_execution_transactions_by_project_daily') }}
  GROUP BY month, project
),
ranked AS (
  SELECT
    month, project, txs, fee_native, gas_used, active_state,
    row_number() OVER (PARTITION BY month ORDER BY txs DESC, fee_native DESC, project ASC) AS rk
  FROM base
),
bucketed AS (
  SELECT
    month,
    if(rk <= 5, project, 'Others') AS project_label,
    txs, fee_native, gas_used, active_state
  FROM ranked
),
monthly AS (
  SELECT
    month,
    project_label AS project,
    sum(txs)                            AS txs,
    sum(fee_native)                     AS fee_native,
    sum(gas_used)                       AS gas_used,
    groupBitmapMergeState(active_state) AS active_state
  FROM bucketed
  GROUP BY month, project_label
)

SELECT * FROM (
  SELECT month AS date, project AS label, 'Transactions'  AS metric, toFloat64(txs)                              AS value FROM monthly
  UNION ALL
  SELECT month AS date, project AS label, 'FeesNative'    AS metric, round(toFloat64(fee_native), 2)             AS value FROM monthly
  UNION ALL
  SELECT month AS date, project AS label, 'GasUsed'       AS metric, toFloat64(gas_used)                         AS value FROM monthly
  UNION ALL
  SELECT
    month AS date,
    project AS label,
    'ActiveSenders' AS metric,
    toFloat64(finalizeAggregation(active_state)) AS value
  FROM monthly
)
ORDER BY date ASC, label ASC, metric ASC