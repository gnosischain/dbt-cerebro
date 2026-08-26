{{
  config(
    materialized='view',
    tags=['production','governance','tier2','api:governance_proposals_live','granularity:latest']
  )
}}

-- Ballots open RIGHT NOW: one row per active proposal whose voting window
-- contains the query time. Freshness is bounded by the daily governance_db
-- ingest, same as every model in this domain -- vote counts can lag up to a
-- day; hours_left is exact (computed from the proposal's own end_at).

SELECT
    id,
    gip_number,
    is_gip,
    title,
    start_at,
    end_at,
    toInt64(dateDiff('hour', now(), end_at))    AS hours_left,
    votes_count,
    scores_total,
    quorum,
    quorum_status,
    scores_total / nullIf(quorum, 0)            AS quorum_ratio,
    toDate(now())                               AS as_of_date
FROM {{ ref('int_governance_proposals') }}
WHERE state = 'active'
  AND end_at > now()
  AND start_at <= now()
ORDER BY end_at ASC
