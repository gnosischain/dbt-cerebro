{% macro governance_phase_bucket(ts_expr, created_expr, start_expr, end_expr) -%}
{#
  Bucket a timestamp into a proposal's lifecycle phase.

  Returns one of:
    pre_discussion - before the Snapshot proposal even existed
    pre_vote       - proposal created, voting not yet open
    voting         - inside the voting window
    post_close     - after voting closed

  These are DERIVED windows from the proposal's own created_at / start_at / end_at,
  deliberately not the DAO's declared phase-1/2/3 tags. The tags cover only ~21% of
  topics overall (76% of GIP-titled ones), while every post and every like carries a
  timestamp -- so the derived windows apply universally. Use the declared tags as a
  secondary dimension where present, never as the primary split.

  post_close is a signal in its own right, not leftover noise: argument arriving after
  the ballot closed is unresolved controversy or regret, and nothing in the platform
  tracks it today.

  Boundaries are half-open and ordered, so a timestamp exactly at start_at counts as
  voting (not pre_vote) and one exactly at end_at counts as post_close. Proposals with
  a start_at before created_at (possible for imported/edited proposals) collapse the
  pre_vote bucket to empty rather than producing a negative window.
#}
multiIf(
    {{ ts_expr }} <  {{ created_expr }}, 'pre_discussion',
    {{ ts_expr }} <  {{ start_expr }},   'pre_vote',
    {{ ts_expr }} <  {{ end_expr }},     'voting',
    'post_close'
)
{%- endmacro %}
