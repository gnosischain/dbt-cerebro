{% macro classify_choice_polarity(choice_expr) -%}
{#
  Classify a Snapshot ballot option label into a voting direction.

  Returns one of: 'abstain' | 'against' | 'for' | 'other'.

  This is the SINGLE source of truth for choice-label polarity. int_governance_proposals
  derives its proposal-level `outcome` from the winning choice using exactly this
  vocabulary (passed <- for, rejected <- against, no_consensus <- abstain,
  decided <- other), and int_governance_vote_choices derives per-voter direction from
  it. Keeping one macro is what stops a per-voter "against" from ever contradicting a
  proposal-level "passed".

  Order matters and mirrors the original int_governance_proposals logic: abstain is
  tested first (an "Abstain" option must never fall through to the 'no' branch on the
  word "no" inside e.g. "Abstain / no preference"), then negative, then affirmative.

  'other' is deliberate, not a failure: selection ballots and unlabeled binary options
  ("Option A" / "Option B") have a real winner that carries no direction. Never coerce
  those to 'for' just because two options existed.

  Equivalence verified 2026-07-28 against the pre-macro logic across all 253 proposals
  in playground_max: passed/for 119, below_quorum/for 78, rejected/against 22,
  open/for 12, below_quorum/against 11, no_consensus/abstain 6, below_quorum/abstain 3,
  decided/other 2 -- no contradictory (outcome, polarity) pair exists.
#}
multiIf(
    match(lower({{ choice_expr }}), '^abstain'), 'abstain',
    match(lower({{ choice_expr }}), '(\\bagainst\\b|\\bno\\b|\\bnay\\b|reject|make no change|do not|\\bdon.?t\\b|do nothing|status quo|not now|\\bnone\\b)'), 'against',
    match(lower({{ choice_expr }}), '(\\bfor\\b|\\byes\\b|approve|adopt|enact|accept|in favou?r|\\baye\\b|agree|support|let.?s do|proceed|enable|extend|launch|activate|ratify)'), 'for',
    'other'
)
{%- endmacro %}
