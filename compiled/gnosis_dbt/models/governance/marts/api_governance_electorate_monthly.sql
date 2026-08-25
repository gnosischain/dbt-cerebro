

-- How big the electorate is, measured two ways at once: how many PEOPLE voted, and how
-- much CAPITAL voted. Plotting both on one chart is the point -- the divergence between
-- the lines is the finding.
--
-- Why this exists. Raw vote counts fell ~98.7% from 2022 (31,157 votes) to 2026 (398), which
-- reads as governance collapse. Normalising kills that story: median voting power per GIP
-- was 137,457 in 2026 versus 101,748 in 2022 -- HIGHER -- and median voters per GIP (68) is
-- back at the pre-airdrop baseline (79 in 2020, 116 in 2021). 2022's 581 voters/GIP was an
-- airdrop-farming bubble, not a healthy norm. The honest summary is that the headcount
-- normalised while the capital never left. Never publish the raw vote-count decline on
-- its own.
--
-- Grain and scoping. One row per month that actually had a GIP proposal. GIP-only
-- (`is_gip = 1`) for the same reason turnout is: 119 of 253 Snapshot proposals are non-GIP
-- spam/phishing averaging ~0.1% turnout, and including them halves every average and makes
-- GIP-free months read as collapse. Months with no GIP proposal are ABSENT rather than
-- zero -- a month with no vote is not a month with no support.
--
-- Medians, not means. Per-proposal participation is heavily skewed by a few airdrop-era
-- ballots (max 1,196 voters), so the median is the honest central value. `*_total` columns
-- are provided for volume questions; do not average them.

WITH per_proposal AS (
    SELECT
        p.id                                       AS proposal_id,
        toStartOfMonth(p.created_at)               AS month,
        uniqExact(v.voter)                         AS voters,
        sum(v.vp_effective)                        AS vp
    FROM `dbt`.`int_governance_proposals` AS p
    INNER JOIN `dbt`.`int_governance_vote_choices` AS v
        ON v.proposal_id = p.id
    WHERE p.is_gip = 1
    GROUP BY p.id, p.created_at
)

SELECT
    month                                          AS date,
    count()                                        AS gip_proposals,
    toUInt64(round(median(voters), 0))             AS median_voters_per_proposal,
    round(median(vp), 0)                           AS median_vp_per_proposal,
    toUInt64(max(voters))                          AS max_voters_per_proposal,
    toUInt64(min(voters))                          AS min_voters_per_proposal,
    toUInt64(sum(voters))                          AS voter_slots_total,
    round(sum(vp), 0)                              AS vp_total
FROM per_proposal
GROUP BY month
ORDER BY date