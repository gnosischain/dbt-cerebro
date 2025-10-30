{{ config(materialized='view', tags=['production','execution','transactions']) }}

WITH base AS (
  SELECT
    t.window,
    t.bucket,
    toFloat64(t.value) AS value
  FROM {{ ref('fct_execution_transactions_by_project_snapshots') }} AS t
  WHERE t.label = 'FeesNative'
    AND t.window IN ('All','7D','30D','90D')
    AND t.bucket IS NOT NULL
),
ranked AS (
  SELECT
    window,
    bucket,
    value,
    row_number() OVER (PARTITION BY window ORDER BY value DESC, bucket ASC) AS rn
  FROM base
),
top AS (
  SELECT window, bucket, value
  FROM ranked
  WHERE rn <= 20
),
others AS (
  SELECT
    window,
    'Others' AS bucket,
    sum(value) AS value
  FROM ranked
  WHERE rn > 20
  GROUP BY window
)
SELECT window AS range, bucket AS label, value FROM top
UNION ALL
SELECT window AS range, bucket AS label, value FROM others WHERE value > 0
ORDER BY range, value DESC