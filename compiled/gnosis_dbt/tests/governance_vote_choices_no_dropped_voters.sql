-- Invariant: every deduped (proposal, voter) pair in staging whose ballot has a
-- supported shape appears in int_governance_vote_choices. Unsupported shapes
-- (declared type vs JSON shape disagreement) legitimately produce no rows; a
-- failing row here is a voter silently dropped from a supported ballot.
-- LEFT ANTI JOIN, not LEFT JOIN + IS NULL: the compound-key null-probe variant
-- silently partial-matches (see api_governance_forum_topic_proposal_links).
SELECT
    v.proposal_id,
    v.voter
FROM `dbt`.`stg_governance__snapshot_votes` AS v
INNER JOIN `dbt`.`int_governance_proposals` AS p
    ON p.id = v.proposal_id
LEFT ANTI JOIN `dbt`.`int_governance_vote_choices` AS c
    ON c.proposal_id = v.proposal_id
   AND c.voter = v.voter
WHERE (p.type IN ('basic', 'single-choice') AND toUInt32OrNull(trim(v.choice_raw)) IS NOT NULL)
   OR (p.type = 'ranked-choice' AND startsWith(trim(v.choice_raw), '['))