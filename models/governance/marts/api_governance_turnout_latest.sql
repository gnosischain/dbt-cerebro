{{
  config(
    materialized='view',
    tags=['production','governance','api:governance_turnout','granularity:latest']
  )
}}

-- Per-proposal turnout browse table. See int_governance_turnout for the full
-- eligible-supply methodology and its historical-precision caveat.
SELECT
    proposal_id,
    gip_number,
    is_gip,
    title,
    outcome,
    category,
    created_at,
    total_vp,
    unique_voters,
    includes_staked_gno,
    eth_circ_supply,
    gnosis_circ_supply,
    staked_gno_component,
    eligible_supply,
    turnout
FROM {{ ref('int_governance_turnout') }}
ORDER BY created_at DESC
