{% set start_month = var('start_month', none) %}
{% set end_month = var('end_month', none) %}
{% set incr_end = mb_var('incremental_end_date') %}
{% set validator_index_start = mb_var('validator_index_start') %}
{% set validator_index_end = mb_var('validator_index_end') %}

{#
  =============================================================================
  RESCOPED 2026-06 — APY is no longer computed here; it is read from
  int_consensus_validators_income_daily.
  =============================================================================
  The previous implementation backed out per-validator income with a
  mod-32-GNO deposit-rounding trick (balance + withdrawn rounded to the nearest
  32 GNO to guess the deposit). That assumes 32-GNO deposit granularity and
  breaks on EIP-7251 (MaxEB) sub-32-GNO top-ups, and it had no consensus-spec
  cap, so a single top-up / consolidation day produced per-validator APY up to
  ~1e47% and down to -100%. Measured on 2026-05-01: 3,884 active validators had
  APY > 200% (1,840 > 1000%) and 12,626 had APY < -50% — versus 0 and 29 with
  the spec-bounded source.

  This model now derives the spec-bounded `apy` straight from
  int_consensus_validators_income_daily, which applies the consensus base-reward
  cap plus ledger-exact effective-credit / consolidation math (see that model's
  header). We deliberately do NOT re-implement the spec math here: this model's
  incremental machinery differs and the income model is the single source of
  truth for income/APY.

  Scope is kept identical to the old model — active_% or pending_queued
  validators only — so the downstream `status != 'pending_queued'` filter and the
  balance distribution in the consumers are unchanged. `balance` stays in gwei so
  the consumers' /POWER(10,9) conversion is untouched. INNER JOIN income means a
  day only appears once the income fact for it has been built (no apy=0 filler
  for days income is still catching up on).

  Consumers (unchanged): int_consensus_validators_dists_daily,
  fct_consensus_validators_dists_last_30_days.

  Incremental: ReplacingMergeTree dedups on (date, validator_index).
  incremental_strategy resolves to `append` when start_month (full-refresh
  batching) or incremental_end_date (microbatch runner) is set; both bound the
  slice via the WHERE clauses below. Otherwise delete+insert re-pulls the recent
  lookback window. Mirrors int_consensus_validators_income_daily so the staged
  full-refresh and microbatch runners drive both identically.
#}
{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if (start_month or incr_end) else 'delete+insert'),
        engine='ReplacingMergeTree()',
        order_by='(date, validator_index)',
        unique_key='(date, validator_index)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        pre_hook=[
            "SET max_bytes_before_external_group_by = 2000000000",
            "SET max_bytes_before_external_sort = 2000000000",
            "SET max_memory_usage = 4000000000",
            "SET join_algorithm = 'grace_hash'",
            "SET grace_hash_join_initial_buckets = 16"
        ],
        post_hook=[
            "SET max_bytes_before_external_group_by = 0",
            "SET max_bytes_before_external_sort = 0",
            "SET max_memory_usage = 0",
            "SET join_algorithm = 'default'"
        ],
        tags=["production", "consensus", "validators_apy", "microbatch"]
    )
}}

{% set range_sql %}
  {% if validator_index_start is not none and validator_index_end is not none %}
    AND validator_index >= {{ validator_index_start }}
    AND validator_index < {{ validator_index_end }}
  {% endif %}
{% endset %}

WITH

-- Per-validator status + balance, scoped to the same active/pending population the
-- old model emitted. balance kept in gwei for the consumers' /POWER(10,9) conversion.
validators AS (
    SELECT
        date
        ,validator_index
        ,status
        ,balance_gwei AS balance
    FROM {{ ref('int_consensus_validators_snapshots_daily') }}
    WHERE date < today()
      AND (status LIKE 'active_%' OR status = 'pending_queued')
    {% if start_month and end_month %}
      AND toStartOfMonth(date) >= toDate('{{ start_month }}')
      AND toStartOfMonth(date) <= toDate('{{ end_month }}')
    {% else %}
      {{ apply_monthly_incremental_filter('date', 'date', 'true', lookback_days=3, filters_sql=range_sql) }}
    {% endif %}
    {% if validator_index_start is not none and validator_index_end is not none %}
      AND validator_index >= {{ validator_index_start }}
      AND validator_index < {{ validator_index_end }}
    {% endif %}
),

-- Spec-bounded per-validator APY (consensus base-reward cap + effective-credit math).
income AS (
    SELECT
        date
        ,validator_index
        ,apy
    FROM {{ ref('int_consensus_validators_income_daily') }}
    WHERE date < today()
    {% if start_month and end_month %}
      AND toStartOfMonth(date) >= toDate('{{ start_month }}')
      AND toStartOfMonth(date) <= toDate('{{ end_month }}')
    {% else %}
      {{ apply_monthly_incremental_filter('date', 'date', 'true', lookback_days=3, filters_sql=range_sql) }}
    {% endif %}
    {% if validator_index_start is not none and validator_index_end is not none %}
      AND validator_index >= {{ validator_index_start }}
      AND validator_index < {{ validator_index_end }}
    {% endif %}
)

SELECT
    v.date AS date
    ,v.validator_index AS validator_index
    ,v.status AS status
    ,v.balance AS balance
    ,i.apy AS apy
FROM validators v
INNER JOIN income i
    ON i.date = v.date
    AND i.validator_index = v.validator_index
