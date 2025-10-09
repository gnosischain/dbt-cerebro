{{ config(
  materialized='view',
  tags=['production','execution','transactions','hourly']
) }}

WITH wd AS (
  SELECT max(hour) AS max_hour
  FROM {{ ref('fct_execution_transactions_active_accounts_hourly_recent') }}
),

base AS (
  SELECT
    h.hour,
    h.active_accounts
  FROM {{ ref('fct_execution_transactions_active_accounts_hourly_recent') }} h
  CROSS JOIN wd
  WHERE toDateTime(h.hour) >  subtractHours(toDateTime(wd.max_hour), 47)
    AND toDateTime(h.hour) <= toDateTime(wd.max_hour)
)

SELECT
  toDateTime(hour) AS date,
  round(
    avg(active_accounts) OVER (
      ORDER BY hour
      ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
    ),
    1
  ) AS value
FROM base
ORDER BY date DESC