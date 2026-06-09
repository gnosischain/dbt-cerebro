{{
  config(
    materialized='view',
    tags=['production', 'execution', 'tier0', 'api:circles_v2_kpi_new_trusts', 'granularity:last_7d', 'window:7d']
  )
}}

-- KPI tile: new trusts granted in the last 7 full days vs prior 7 days.

WITH recent AS (
    SELECT
        sum(n_new_trusts)      AS n_new,
        sum(n_revoked_trusts)  AS n_revoked
    FROM {{ ref('int_execution_circles_v2_trusts_daily') }}
    WHERE date >= today() - 7
      AND date <  today()
),
prior AS (
    SELECT
        sum(n_new_trusts)      AS n_new
    FROM {{ ref('int_execution_circles_v2_trusts_daily') }}
    WHERE date >= today() - 14
      AND date <  today() - 7
)

SELECT
    coalesce(r.n_new,     0)                                            AS value,
    coalesce(r.n_revoked, 0)                                            AS revoked,
    round((toFloat64(r.n_new) - toFloat64(p.n_new))
          / nullIf(toFloat64(p.n_new), 0) * 100, 1)                     AS change_pct
FROM recent r
CROSS JOIN prior p
