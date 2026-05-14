{{ config(materialized='table') }}

-- ISO-week-aligned time spine. Each row is a Monday-start week between
-- Gnosis Chain genesis (2018-10-08, which was itself a Monday) and 5
-- years past today. Built as a deduped projection of dim_time_spine_daily
-- so granularity boundaries always agree (no off-by-one when joining
-- weekly metrics via the spine).

SELECT DISTINCT
    toMonday(day) AS week
FROM {{ ref('dim_time_spine_daily') }}
ORDER BY week
