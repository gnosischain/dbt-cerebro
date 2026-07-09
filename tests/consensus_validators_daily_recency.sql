-- Recency guard for the consensus validators daily aggregates.
--
-- If the external beacon crawler that writes the consensus.validators source
-- stalls, these three daily tables freeze at the last ingested day. That failure
-- is otherwise SILENT: every schema test on the consensus tree is windowed to the
-- last {{ var('test_lookback_days', 7) }} days (which becomes an empty set once no
-- new rows land, so the tests pass vacuously), and the quarterly marts resolve
-- "current" via argMax(value, date) -- which just returns the last row that
-- exists, serving a stale mid-quarter value as if it were quarter-end. This
-- happened 2026-06-07 (staked_gno/validators_active silently served the June-7
-- snapshot for a month; see docs/model_review/q2_2026_report_v2_crosscheck.md).
--
-- This check has NO date window and NO staleness-blind filter, so it fires as soon
-- as the tables stop advancing. Passing = zero rows. Threshold is var-overridable.
--
-- NOTE: like tests/contracts_live_tables_freshness.sql, a standalone tests/*.sql is
-- NOT auto-selected by run_dbt_observability.sh's model-path batches. To make it
-- block the daily/preview cron, add a selector for tag:consensus,tag:freshness to
-- that script; until then run manually:
--   dbt test --select consensus_validators_daily_recency
{{ config(tags=['production', 'consensus', 'freshness']) }}
{% set error_after_days = var('consensus_recency_error_after_days', 2) %}

{% set daily_tables = [
    'int_consensus_validators_balances_daily',
    'int_consensus_validators_status_daily',
    'int_consensus_validators_snapshots_daily'
] %}

{% for t in daily_tables %}
SELECT
    '{{ t }}'                                        AS table_name
    ,max(toDate(date))                               AS max_date
    ,dateDiff('day', max(toDate(date)), today())     AS days_stale
FROM {{ ref(t) }}
HAVING days_stale > {{ error_after_days }}
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
