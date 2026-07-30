{{
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='(proposal_id, topic_id)',
    tags=['production','intermediate','governance']
  )
}}

-- The unified forum-topic <-> Snapshot-proposal bridge: one row per (proposal, topic)
-- pair, with which signal(s) established the link. Everything phase-related joins
-- through here rather than re-deriving a bridge.
--
-- Two independent signals, unioned (measured 2026-07-28 against 253 proposals):
--   gip_number - the topic title and the proposal title carry the same GIP number.
--                Links 133 proposals. The dominant signal.
--   post_link  - a post in the topic pastes the proposal's own 0x URL.
--                Links 95 proposals, and reaches non-GIP proposals the number match
--                cannot.
-- Union: 137 of 253 proposals (54%), covering effectively all 134 GIP proposals plus a
-- few non-GIP ones, across 339 pairs and 182 topics.
--
-- Deliberately NOT used as a third signal: the proposal's own `discussion` URL field.
-- It only parses for 89 proposals and is a strict subset of what the two signals above
-- already reach, so adding it would widen the code without widening coverage. (An
-- earlier draft of the design quoted that 89 as if it were total coverage -- it is the
-- weakest of the three, not the only one.)
--
-- MANY-TO-MANY BY DESIGN. A topic links to several proposals when a GIP is re-balloted
-- (GIPs 74/87/91/134 have redos), and a proposal links to several topics when discussion
-- spans threads. Do not assume one row per proposal or per topic. Downstream models that
-- need a single proposal per topic must pick one explicitly -- e.g. via
-- int_governance_gip's canonical outcome resolution.

WITH via_gip AS (
    SELECT
        p.id                        AS proposal_id,
        t.id                        AS topic_id,
        CAST('gip_number' AS String) AS method
    FROM {{ ref('int_governance_proposals') }} AS p
    INNER JOIN {{ ref('stg_governance__forum_topics') }} AS t
        ON t.gip_number = p.gip_number
    WHERE p.gip_number > 0
),

via_post AS (
    SELECT
        assumeNotNull(proposal_id)  AS proposal_id,
        topic_id,
        CAST('post_link' AS String) AS method
    FROM {{ ref('int_governance_forum_topic_proposal_links') }}
    WHERE proposal_id IS NOT NULL
      AND proposal_matched = 1
),

combined AS (
    SELECT proposal_id, topic_id, method FROM via_gip
    UNION ALL
    SELECT proposal_id, topic_id, method FROM via_post
),

grouped AS (
    SELECT
        proposal_id,
        topic_id,
        groupUniqArray(method) AS link_methods
    FROM combined
    GROUP BY proposal_id, topic_id
)

SELECT
    proposal_id,
    topic_id,
    link_methods,
    has(link_methods, 'gip_number')                        AS via_gip_number,
    has(link_methods, 'post_link')                         AS via_post_link,
    -- Both signals agreeing is a strength indicator, not a requirement. A single-signal
    -- link is still a link; false here means one signal fired, not that they disagreed.
    length(link_methods) > 1                               AS corroborated
FROM grouped
