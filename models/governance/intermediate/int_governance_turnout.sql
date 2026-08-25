{{
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='(created_at, proposal_id)',
    tags=['production','governance','turnout']
  )
}}

-- Per-proposal voting-power turnout: cast vp / eligible vp.
--
-- Eligible vp = circulating GNO on both chains (crawlers_data.dune_gno_supply,
-- 'Ethereum Circ. Supply' + 'Gnosis Circ. Supply' -- excludes 'Non-Circ.
-- Supply': non-circulating GNO -- treasury/locked/vesting -- cannot vote)
-- plus staked GNO (api_consensus_staked_daily), but the staked component is
-- included ONLY when THIS proposal's own strategy_names contains
-- 'beacon-chain'. Different eras used different strategy sets -- 219/253
-- proposals use an older 'gno' strategy that predates/excludes staking as an
-- eligible pool -- so eligibility is read per-proposal from data already
-- captured, not assumed from a hardcoded date cutoff. Verified: e.g. a 2022
-- proposal shows real staked GNO existed (108k) but its own strategies didn't
-- include beacon-chain, so it is correctly excluded from that proposal's
-- denominator.
--
-- Matched by date (day grain, proposal's created_at): verified 0 of 253
-- proposals miss an exact date match against 5+ years of daily supply data,
-- so no ASOF/nearest-date join is needed.
--
-- Numerator (total_vp) is already delegation-aware -- Snapshot resolves a
-- delegator's power into their delegate's cast vote before it reaches this
-- data -- so turnout does not depend on the (separate) delegate graph.
--
-- Historical precision caveat: only the CURRENT getGnoVotingPower contract
-- was independently verified as a simple balance passthrough. The older
-- 'gno' strategy variant (used by most historical proposals) was not audited
-- the same way -- treat pre-2022-era turnout as directional, not exact.
--
-- Scoped to is_gip=1 (real GIPs) only. Non-GIP Snapshot activity (spam /
-- phishing-style announcement posts, e.g. fake "$GNO Airdrop Program"
-- proposals) is 47% of all proposals by row count but averages ~0.1%
-- turnout -- left unfiltered it roughly halves the blended average and can
-- make a real-GIP-free month read as "turnout collapsed to 0%."
WITH supply_wide AS (
    SELECT
        date,
        sumIf(supply, label = 'Ethereum Circ. Supply') AS eth_circ_supply,
        sumIf(supply, label = 'Gnosis Circ. Supply')   AS gnosis_circ_supply
    FROM {{ ref('api_crawlers_data_gno_supply_daily') }}
    GROUP BY date
),
staked AS (
    SELECT date, value AS staked_gno
    FROM {{ ref('api_consensus_staked_daily') }}
)
SELECT
    p.id                                   AS proposal_id,
    p.gip_number,
    p.is_gip,
    p.title,
    p.outcome,
    p.category,
    p.created_at,
    toDate(p.created_at)                   AS proposal_date,
    p.total_vp,
    p.unique_voters,
    has(p.strategy_names, 'beacon-chain')  AS includes_staked_gno,
    coalesce(s.eth_circ_supply, 0)         AS eth_circ_supply,
    coalesce(s.gnosis_circ_supply, 0)      AS gnosis_circ_supply,
    if(has(p.strategy_names, 'beacon-chain'), coalesce(st.staked_gno, 0), 0) AS staked_gno_component,
    coalesce(s.eth_circ_supply, 0) + coalesce(s.gnosis_circ_supply, 0)
        + if(has(p.strategy_names, 'beacon-chain'), coalesce(st.staked_gno, 0), 0) AS eligible_supply,
    round(
        p.total_vp / nullIf(
            coalesce(s.eth_circ_supply, 0) + coalesce(s.gnosis_circ_supply, 0)
                + if(has(p.strategy_names, 'beacon-chain'), coalesce(st.staked_gno, 0), 0),
            0
        ), 4
    ) AS turnout
FROM {{ ref('int_governance_proposals') }} p
LEFT JOIN supply_wide s ON s.date = toDate(p.created_at)
LEFT JOIN staked st ON st.date = toDate(p.created_at)
WHERE p.is_gip = 1
