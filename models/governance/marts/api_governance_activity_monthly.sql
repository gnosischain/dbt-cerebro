{{
  config(
    materialized='view',
    tags=['production','governance','api:governance_activity','granularity:monthly']
  )
}}

-- Monthly governance activity, long format (date, metric, value).
SELECT toStartOfMonth(created_at) AS date, 'proposals_created' AS metric, toUInt64(count()) AS value
FROM {{ ref('int_governance_proposals') }}
WHERE created_at > toDateTime('2020-01-01 00:00:00', 'UTC')
GROUP BY date

UNION ALL

SELECT toStartOfMonth(created_at) AS date, 'votes_cast' AS metric, toUInt64(count()) AS value
FROM {{ ref('stg_governance__snapshot_votes') }}
WHERE created_at > toDateTime('2020-01-01 00:00:00', 'UTC')
GROUP BY date

UNION ALL

SELECT toStartOfMonth(created_at) AS date, 'unique_voters' AS metric, toUInt64(uniqExact(voter)) AS value
FROM {{ ref('stg_governance__snapshot_votes') }}
WHERE created_at > toDateTime('2020-01-01 00:00:00', 'UTC')
GROUP BY date

ORDER BY date, metric
