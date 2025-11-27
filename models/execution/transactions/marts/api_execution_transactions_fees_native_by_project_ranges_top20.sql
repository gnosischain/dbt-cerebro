{{ config(materialized='view', tags=['production','execution','transactions', 'tier0', 'api: fees_by_project_ranges_top20']) }}

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
)
SELECT
  window AS range,
  if(rn <= 20, bucket, 'Others') AS label,
  sum(value) AS value
FROM ranked
GROUP BY range, label
HAVING value > 0
ORDER BY
  multiIf(range = 'All', 1, range = '90D', 2, range = '30D', 3, range = '7D', 4, 5),
  value DESC,
  label ASC