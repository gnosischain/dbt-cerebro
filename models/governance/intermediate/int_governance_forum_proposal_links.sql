{{
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='(topic_id, post_number, snapshot_proposal_id)',
    tags=['production','governance','forum']
  )
}}

-- Cross-link: forum post → Snapshot proposal via a pasted proposal/0x… URL.
-- This is NOT GIP identity (that stays on titles / int_governance_gip). Many
-- valid threads never paste a Snapshot link; unmatched ids (other spaces,
-- stale links) stay with proposal_matched = 0 and null proposal fields.
SELECT
    p.id                                                    AS post_id,
    p.topic_id,
    p.post_number,
    p.user_id,
    p.username,
    p.created_at                                            AS post_created_at,
    -- Non-null after WHERE; assumeNotNull so MergeTree order_by is legal.
    assumeNotNull(p.snapshot_proposal_id)                   AS snapshot_proposal_id,
    pr.id IS NOT NULL                                       AS proposal_matched,
    pr.id                                                   AS proposal_id,
    pr.gip_number                                           AS proposal_gip_number,
    pr.title                                                AS proposal_title,
    pr.state                                                AS proposal_state,
    pr.outcome                                              AS proposal_outcome,
    pr.author                                               AS proposal_author,
    pr.created_at                                           AS proposal_created_at,
    pr.unique_voters                                        AS proposal_unique_voters
FROM {{ ref('stg_governance__forum_posts') }} AS p
LEFT JOIN {{ ref('int_governance_proposals') }} AS pr
    ON p.snapshot_proposal_id = lower(pr.id)
WHERE p.snapshot_proposal_id IS NOT NULL
