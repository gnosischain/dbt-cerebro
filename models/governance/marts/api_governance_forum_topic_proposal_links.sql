{{
  config(
    materialized='view',
    tags=['production','governance','api:governance_forum_proposal_links','granularity:latest']
  )
}}

-- Forum discussion <-> Snapshot vote cross-links, topic grain. Surfaces
-- discussions that link to a real ballot via a pasted proposal/0x... URL --
-- including topics where title-parsing found no GIP number at all but a
-- linked post recovers the connection (recovered_via_link; 88 such topics
-- verified). Not GIP identity -- a topic can have its own gip_number and/or
-- a linked proposal_gip_number, and they are not always the same value.
SELECT
    l.topic_id,
    t.title                                                       AS topic_title,
    t.gip_number                                                  AS topic_gip_number,
    t.phase                                                       AS topic_phase,
    l.snapshot_proposal_id,
    l.proposal_matched,
    l.proposal_id,
    l.proposal_gip_number,
    l.proposal_title,
    l.proposal_state,
    l.proposal_outcome,
    l.linking_posts,
    l.first_linked_at,
    l.last_linked_at,
    t.gip_number IS NULL AND l.proposal_gip_number IS NOT NULL     AS recovered_via_link
FROM {{ ref('int_governance_forum_topic_proposal_links') }} l
LEFT JOIN {{ ref('stg_governance__forum_topics') }} t ON l.topic_id = t.id
