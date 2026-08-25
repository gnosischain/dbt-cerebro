{{
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='(proposal_id, voter, strategy_index)',
    tags=['production','governance','power']
  )
}}

-- Explodes each vote's vp_by_strategy against the PROPOSAL's own strategy set
-- (positional), classifying each component into a power source. Empty until
-- proposals are re-ingested with the `strategies` field (strategy_names empty
-- -> range() empty -> no ARRAY JOIN rows, no error).
SELECT
    v.proposal_id,
    v.voter,
    p.gip_number,
    p.created_at AS proposal_created_at,
    p.end_at     AS proposal_end_at,
    p.outcome,
    tupleElement(s, 1) AS strategy_index,
    tupleElement(s, 2) AS strategy_name,
    tupleElement(s, 3) AS strategy_network,
    tupleElement(s, 4) AS strategy_vp,
    -- Strategy names vary across proposals: beacon-chain (staking), delegation
    -- / erc20-balance-of-delegation (delegated), and gno / erc20-balance-of /
    -- contract-call (GNO balance). Bucket into three sources; strategy_network
    -- is kept as a separate column for chain-level drill-down.
    multiIf(
        tupleElement(s, 2) = 'beacon-chain',                                'Staked GNO (GBC)',
        position(tupleElement(s, 2), 'delegation') > 0,                     'Delegated',
        tupleElement(s, 2) IN ('gno', 'erc20-balance-of', 'contract-call'), 'GNO holdings',
        tupleElement(s, 2)
    ) AS power_source
FROM {{ ref('stg_governance__snapshot_votes') }} v
INNER JOIN {{ ref('int_governance_proposals') }} p ON v.proposal_id = p.id
ARRAY JOIN arrayMap(
    i -> (toUInt16(i), p.strategy_names[i], p.strategy_networks[i], v.vp_by_strategy[i]),
    range(1, least(length(p.strategy_names), length(v.vp_by_strategy)) + 1)
) AS s
