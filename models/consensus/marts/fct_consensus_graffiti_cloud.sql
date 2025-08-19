{{ 
    config(
        materialized='table'
    ) 
}}


WITH base AS (
  SELECT
      graffiti,
      sumIf(cnt, date >= today() - 7)  AS v_7d,
      sumIf(cnt, date >= today() - 30) AS v_30d,
      sumIf(cnt, date >= today() - 90) AS v_90d,
      sum(cnt)                         AS v_all
  FROM {{ ref('int_consensus_graffiti_daily') }}
  WHERE graffiti != 'None'
  GROUP BY graffiti
)
SELECT
  label,
  graffiti,
  value
FROM base
ARRAY JOIN
  ['7D','30D','90D','All'] AS label,
  [v_7d, v_30d, v_90d, v_all] AS value
ORDER BY label, value DESC
LIMIT 50 BY label