{{
  config(
    materialized='view',
    tags=['production','governance','api:governance_turnout_monthly','granularity:monthly']
  )
}}

-- Monthly turnout trend: a simple average of per-proposal turnout ratios,
-- NOT a population-weighted average (a month with one large-eligible-supply
-- proposal and one small one weighs them equally). Directional trend only --
-- for exact per-vote figures use api_governance_turnout_latest.
SELECT
    toStartOfMonth(created_at) AS date,
    round(avg(turnout), 4)     AS avg_turnout,
    count()                    AS proposals
FROM {{ ref('int_governance_turnout') }}
GROUP BY date
ORDER BY date
