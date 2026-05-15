{{ config(materialized='table') }}

-- Calendar-month-aligned time spine. Each row is the first day of a
-- month between Gnosis Chain genesis (2018-10-01 — month boundary
-- preceding the 2018-10-08 chain start) and 5 years past today. Built
-- as a deduped projection of dim_time_spine_daily so granularity
-- boundaries always agree.

SELECT DISTINCT
    toStartOfMonth(day) AS month
FROM {{ ref('dim_time_spine_daily') }}
ORDER BY month
