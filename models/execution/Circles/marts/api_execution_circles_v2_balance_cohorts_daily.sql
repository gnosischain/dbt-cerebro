{{
  config(
    materialized='view',
    tags=['production','execution','circles_v2','api:circles_v2_balance_cohorts_daily','granularity:daily']
  )
}}

-- Daily distribution of CRC holders across balance buckets (wealth distribution).
-- Passthrough over int_execution_circles_v2_balance_cohorts_daily, excluding the current incomplete day.
SELECT
    date,
    balance_bucket,
    holder_count,
    total_balance,
    total_demurraged_balance
FROM {{ ref('int_execution_circles_v2_balance_cohorts_daily') }}
WHERE date < today()
