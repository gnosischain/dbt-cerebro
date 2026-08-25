{{
  config(
    materialized='view',
    tags=['production','staging','governance']
  )
}}

-- One latest vote per (proposal_id, voter). A voter re-voting on an open
-- proposal can create a new vote id, so the raw table may hold more than one
-- row per voter; keep the most recent by the vote's own timestamp (tie-break
-- on ingested_at). This makes revotes correct without re-keying the raw table.
SELECT
    id,
    proposal_id,
    space_id,
    voter,
    created_at,
    vp,
    vp_state,
    -- choice shape depends on ballot type (int for basic/single-choice,
    -- array/object for approval/ranked/weighted) — keep raw for downstream.
    JSONExtractRaw(raw_json, 'choice')                        AS choice_raw,
    -- Voting power split across the PROPOSAL's strategies, positional.
    JSONExtract(raw_json, 'vp_by_strategy', 'Array(Float64)') AS vp_by_strategy,
    JSONExtractString(raw_json, 'reason')                     AS reason
FROM (
    SELECT
        id, proposal_id, space_id, lower(voter) AS voter,
        created_at, vp, vp_state, raw_json,
        row_number() OVER (
            PARTITION BY proposal_id, lower(voter)
            ORDER BY created_at DESC, ingested_at DESC
        ) AS rn
    FROM {{ source('governance', 'snapshot_votes') }} FINAL
)
WHERE rn = 1
