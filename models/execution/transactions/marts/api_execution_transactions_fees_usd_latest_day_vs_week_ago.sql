-- this is an example if we wanted to have instead the latest day and compare to that day one week before

{{ config(materialized='view', tags=['production','execution','transactions']) }}

WITH wd AS (
  SELECT max(date) AS max_date
  FROM {{ ref('int_execution_transactions_info_daily') }}
),
d AS (
  SELECT
    date,
    sumIf(fee_usd_sum, success = 1) AS fee_usd
  FROM {{ ref('int_execution_transactions_info_daily') }}
  GROUP BY date
),
curr AS (
  SELECT toFloat64(d.fee_usd) AS value
  FROM wd
  LEFT JOIN d ON d.date = wd.max_date
),
prev AS (
  SELECT toFloat64(d.fee_usd) AS value
  FROM wd
  LEFT JOIN d ON d.date = toDate(subtractDays(wd.max_date, 7))
)
SELECT
  curr.value                                                AS value,
  round((coalesce(curr.value / nullIf(prev.value, 0), 0) - 1) * 100, 1) AS change_pct
FROM curr
CROSS JOIN prev