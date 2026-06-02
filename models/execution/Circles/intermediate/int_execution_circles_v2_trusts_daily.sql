{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    engine='ReplacingMergeTree()',
    order_by='date',
    partition_by='toStartOfMonth(date)',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','circles_v2','trusts','daily']
  )
}}

-- Event-grain daily trust activity (complements fct_execution_circles_v2_active_trusts_daily
-- which carries the SCD2-derived net active stock from int_execution_circles_v2_trust_pair_ranges).
--
--   n_trust_events     - total Trust events on this day
--   n_new_trusts       - events with expiry > block_timestamp (trust granted/extended)
--   n_revoked_trusts   - events with expiry <= block_timestamp (set to 0 = revoke)
--   n_distinct_trusters - distinct truster addresses active that day
--   n_distinct_trustees - distinct trustee addresses active that day

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

SELECT
    toDate(block_timestamp)                                              AS date,
    count()                                                              AS n_trust_events,
    countIf(expiry_time >  block_timestamp)                              AS n_new_trusts,
    countIf(expiry_time <= block_timestamp)                              AS n_revoked_trusts,
    uniqExact(truster)                                                   AS n_distinct_trusters,
    uniqExact(trustee)                                                   AS n_distinct_trustees
FROM {{ ref('int_execution_circles_v2_trust_updates') }}
WHERE block_timestamp < today()
  {% if start_month and end_month %}
    AND toStartOfMonth(toDate(block_timestamp)) >= toDate('{{ start_month }}')
    AND toStartOfMonth(toDate(block_timestamp)) <= toDate('{{ end_month }}')
  {% else %}
    {{ apply_monthly_incremental_filter(
          source_field='block_timestamp',
          destination_field='date',
          add_and=True) }}
  {% endif %}
GROUP BY date
