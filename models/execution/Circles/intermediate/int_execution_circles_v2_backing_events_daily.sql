{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    engine='ReplacingMergeTree()',
    order_by='(date, lifecycle_stage)',
    partition_by='toStartOfMonth(date)',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','circles_v2','backing','daily']
  )
}}

-- Daily Circles v2 backing-lifecycle event counts. Tracks the "depositors" set —
-- addresses that emit a backing event. This is the *transactional* population
-- (not the trust-defined "backers" set, which awaits the backers-group address).
--
--   n_events           - total events in this stage on this day
--   n_distinct_backers - distinct `backer` addresses
--   n_distinct_assets  - distinct backing assets pledged

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

SELECT
    toDate(block_timestamp)                     AS date,
    lifecycle_stage                             AS lifecycle_stage,
    count()                                     AS n_events,
    uniqExactIf(backer, backer IS NOT NULL)     AS n_distinct_backers,
    uniqExactIf(backing_asset, backing_asset IS NOT NULL) AS n_distinct_assets
FROM {{ ref('int_execution_circles_v2_backing') }}
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
GROUP BY date, lifecycle_stage
