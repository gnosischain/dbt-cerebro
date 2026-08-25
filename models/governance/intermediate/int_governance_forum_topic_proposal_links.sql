{{
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='(topic_id, snapshot_proposal_id)',
    tags=['production','governance','forum']
  )
}}

-- Topic-level rollup of Snapshot links found in any post of the thread.
-- One row per (topic, proposal id). linking_posts = how many posts in the
-- topic pasted that URL. Use with stg/int topics; does not invent GIP numbers.
WITH links AS (
    SELECT
        topic_id,
        snapshot_proposal_id,
        toUInt8(proposal_matched) AS matched_u8,
        proposal_id,
        proposal_gip_number,
        proposal_title,
        proposal_state,
        proposal_outcome,
        post_created_at
    FROM {{ ref('int_governance_forum_proposal_links') }}
)

SELECT
    topic_id,
    snapshot_proposal_id,
    max(matched_u8) = 1                                         AS proposal_matched,
    nullIf(argMax(proposal_id, matched_u8), '')                 AS proposal_id,
    argMax(proposal_gip_number, matched_u8)                     AS proposal_gip_number,
    nullIf(argMax(proposal_title, matched_u8), '')              AS proposal_title,
    nullIf(argMax(proposal_state, matched_u8), '')              AS proposal_state,
    nullIf(argMax(proposal_outcome, matched_u8), '')            AS proposal_outcome,
    count()                                                     AS linking_posts,
    min(post_created_at)                                        AS first_linked_at,
    max(post_created_at)                                        AS last_linked_at
FROM links
GROUP BY
    topic_id,
    snapshot_proposal_id
