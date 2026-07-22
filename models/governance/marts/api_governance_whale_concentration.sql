{{
  config(
    materialized='view',
    tags=['production','governance','api:governance_whale_concentration','granularity:latest']
  )
}}

-- Per-proposal voting-power concentration: share of total vp cast by the
-- top 1 / 5 / 10 voters. Reveals whale-decided votes that a raw voter count
-- hides -- e.g. GIP-71 has 1,196 voters yet one address holds ~93% of the
-- cast vp (verified). NULL shares on zero-vote proposals (total_vp = 0).
WITH ranked AS (
    SELECT
        proposal_id,
        vp,
        row_number() OVER (PARTITION BY proposal_id ORDER BY vp DESC) AS vp_rank
    FROM {{ ref('stg_governance__snapshot_votes') }}
),
agg AS (
    SELECT
        proposal_id,
        sumIf(vp, vp_rank = 1)   AS top1_vp,
        sumIf(vp, vp_rank <= 5)  AS top5_vp,
        sumIf(vp, vp_rank <= 10) AS top10_vp
    FROM ranked
    GROUP BY proposal_id
)
SELECT
    p.id                                          AS proposal_id,
    p.gip_number,
    p.is_gip,
    p.title,
    p.outcome,
    p.category,
    p.created_at,
    p.unique_voters,
    p.total_vp,
    coalesce(a.top1_vp, 0.0)                      AS top1_vp,
    round(a.top1_vp  / nullIf(p.total_vp, 0), 4)  AS top1_share,
    coalesce(a.top5_vp, 0.0)                      AS top5_vp,
    round(a.top5_vp  / nullIf(p.total_vp, 0), 4)  AS top5_share,
    coalesce(a.top10_vp, 0.0)                     AS top10_vp,
    round(a.top10_vp / nullIf(p.total_vp, 0), 4)  AS top10_share
FROM {{ ref('int_governance_proposals') }} p
LEFT JOIN agg a ON p.id = a.proposal_id
