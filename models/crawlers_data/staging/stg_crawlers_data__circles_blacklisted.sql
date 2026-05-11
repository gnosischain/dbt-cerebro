{{
  config(
    materialized='view',
    tags=['production','staging','crawlers_data','circles_v2','blacklist']
  )
}}

-- Current Circles v2 blacklist, normalised for downstream joins.
--
-- The upstream `crawlers_data.circles_blacklisted` table is fully replaced
-- on every click-runner run, so this view always reflects the latest known
-- state. Avatar joins in dbt should `lower()` their address column to match.

SELECT
    lower(address) AS address,
    reason,
    ingested_at
FROM {{ source('crawlers_data', 'circles_blacklisted') }}
