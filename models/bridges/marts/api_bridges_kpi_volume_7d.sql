{{ config(materialized='view', tags=['production','bridges','api']) }}
SELECT
  round(vol_7d, 2)        AS value,
  round(vol_prev_7d, 2)   AS prev_value,
  chg_vol_7d              AS change_pct
FROM {{ ref('fct_bridges_kpis_snapshot') }}
ORDER BY as_of_date DESC
LIMIT 1