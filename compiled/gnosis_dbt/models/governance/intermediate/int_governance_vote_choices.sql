

-- One row per (proposal, voter, selected choice): Snapshot's polymorphic raw `choice`
-- resolved to a human-readable label and a For/Against/Abstain direction. This is the
-- foundation every direction and contestation metric reads -- nothing downstream should
-- parse `choice_raw` again.
--
-- Ballot shapes. `choice` is typed differently per ballot `type` (verified distribution
-- for gnosis.eth on 2026-07-28: basic 245, single-choice 7, ranked-choice 1):
--   basic / single-choice -> a 1-BASED integer index into choices[]  -> exactly one row
--   ranked-choice         -> an array of 1-based indices, best first -> one row per rank
--   approval / weighted / quadratic -> NOT currently used by this space. They are
--       classified 'unsupported' and produce NO rows, rather than being guessed at.
--       That silent drop is deliberate but must never go unnoticed, so the
--       accepted_values test on ballot_type (schema.yml) pins the decodable set to
--       basic / single-choice / ranked-choice. It fires the moment gnosis.eth enables
--       a type this model cannot handle, before voters can quietly disappear from the
--       direction metrics. If it fails, add handling here -- do not widen the list.
--
-- On trusting a positional index. Indexing choices[] by the vote's integer is Snapshot's
-- documented contract, and per the event-struct-array-decode lesson we did not take that
-- positional map on faith: on 2026-07-28 every proposal's scores[] was reconstructed by
-- summing vp_effective per choice index and reconciled against the proposal's OWN
-- reported scores[] -- an independent authority computed by Snapshot, not by us. Exact
-- match across all basic/single-choice proposals. Kept as a documented one-off rather
-- than a standing test (tests/ here is reserved for heavyweight financial
-- reconciliations); redo it by hand if the ballot handling changes.
--
-- Polarity comes from the shared classify_choice_polarity macro, the same vocabulary
-- int_governance_proposals uses for its outcome, so per-voter direction can never
-- disagree with the proposal-level result.

WITH v AS (
    SELECT
        s.proposal_id,
        s.voter,
        s.vp,
        s.created_at,
        s.reason,
        s.choice_raw,
        p.type       AS ballot_type,
        p.choices    AS choices,
        p.gip_number,
        p.is_gip,
        p.created_at AS proposal_created_at,
        p.start_at,
        p.end_at
    FROM `dbt`.`stg_governance__snapshot_votes` AS s
    INNER JOIN `dbt`.`int_governance_proposals` AS p
        ON p.id = s.proposal_id
),

-- Shape detection is driven by BOTH the declared ballot type and the actual JSON shape.
-- A type that claims 'basic' but carries an array is treated as unsupported rather than
-- coerced -- disagreement between the two is exactly the case where guessing is unsafe.
kinded AS (
    SELECT
        *,
        multiIf(
            ballot_type IN ('basic', 'single-choice')
                AND toUInt32OrNull(trim(choice_raw)) IS NOT NULL,  'single',
            ballot_type = 'ranked-choice'
                AND startsWith(trim(choice_raw), '['),             'ranked',
            'unsupported'
        ) AS choice_kind
    FROM v
),

-- Normalise both shapes to one array so a single ARRAY JOIN handles them.
indexed AS (
    SELECT
        *,
        multiIf(
            choice_kind = 'single', [toUInt32OrZero(trim(choice_raw))],
            choice_kind = 'ranked', JSONExtract(choice_raw, 'Array(UInt32)'),
            CAST([], 'Array(UInt32)')
        ) AS choice_indexes
    FROM kinded
)

SELECT
    proposal_id,
    gip_number,
    is_gip,
    voter,
    vp,
    created_at,
    proposal_created_at,
    start_at,
    end_at,
    ballot_type,
    choice_kind,
    reason,
    rank_pos     AS preference_rank,
    choice_index,
    -- Out-of-range indices are flagged, never silently mapped to a neighbouring option.
    (choice_index >= 1 AND choice_index <= length(choices)) AS choice_index_valid,
    -- arrayElement THROWS on index 0, so the lookup is clamped to >= 1. The clamped
    -- value is only ever read when choice_index_valid is true, so the clamp cannot
    -- fabricate a label; it exists purely to keep a malformed row from killing the build.
    if(
        choice_index >= 1 AND choice_index <= length(choices),
        arrayElement(choices, toUInt32(greatest(choice_index, 1))),
        ''
    )                                                       AS choice_label,
    if(
        choice_index >= 1 AND choice_index <= length(choices),
        
multiIf(
    match(lower(arrayElement(choices, toUInt32(greatest(choice_index, 1)))), '^abstain'), 'abstain',
    match(lower(arrayElement(choices, toUInt32(greatest(choice_index, 1)))), '(\\bagainst\\b|\\bno\\b|\\bnay\\b|reject|make no change|do not|\\bdon.?t\\b|do nothing|status quo|not now|\\bnone\\b)'), 'against',
    match(lower(arrayElement(choices, toUInt32(greatest(choice_index, 1)))), '(\\bfor\\b|\\byes\\b|approve|adopt|enact|accept|in favou?r|\\baye\\b|agree|support|let.?s do|proceed|enable|extend|launch|activate|ratify)'), 'for',
    'other'
),
        'unknown'
    )                                                       AS polarity,
    -- Voting power attributable to THIS row. A single-choice voter puts their whole vp
    -- behind one option. Ranked-choice vp is not divisible across preferences -- Snapshot
    -- resolves ranked ballots by instant-runoff, not by splitting weight -- so only the
    -- first preference carries vp and later ranks carry 0. Summing vp_effective therefore
    -- reconstructs scores[] for single-choice ballots and never double-counts a ranked voter.
    if(choice_kind = 'ranked' AND rank_pos > 1, 0.0, vp)    AS vp_effective
FROM indexed
ARRAY JOIN
    choice_indexes                 AS choice_index,
    arrayEnumerate(choice_indexes) AS rank_pos
WHERE length(choice_indexes) > 0