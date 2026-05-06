{#
  build_journey_lookback(lookback_days, sector)

  Generates the body of an `fct_<sector>_journeys_<N>d` mart from a single
  template. Wraps the conversion → events join with a leakage guard
  (event_ts < conversion_ts) and a lookback window.

  sector ∈ {'gnosis_app', 'gpay'}. The two sectors share the same shape;
  GP just additionally matches on identity_role (so treasury-grain
  conversions match treasury-grain touchpoints, etc.).

  Each variant model is a 3-line wrapper, e.g.:

    -- models/execution/gnosis_app/marts/fct_execution_gnosis_app_journeys_30d.sql
    {{ build_journey_lookback(30, 'gnosis_app') }}

    -- models/execution/gpay/marts/fct_execution_gpay_journeys_30d.sql
    {{ build_journey_lookback(30, 'gpay') }}

  This single macro powers both the live 30d marts and the deferred
  7d/14d/60d sensitivity-sweep variants.
#}
{% macro build_journey_lookback(lookback_days, sector) %}
{% set conversion_model = 'int_execution_' ~ sector ~ '_conversions' %}
{% set events_model = 'int_execution_' ~ sector ~ '_user_events_unified' %}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

{{ config(
    materialized='incremental',
    incremental_strategy=('append' if start_month else 'delete+insert'),
    engine='ReplacingMergeTree()',
    order_by='(conversion_date, conversion_kind, user_pseudonym, conversion_ts, touch_ts)',
    unique_key=(
      '(conversion_ts, conversion_kind, user_pseudonym, identity_role, touch_ts, event_kind, event_dedup_key)'
      if sector == 'gpay'
      else '(conversion_ts, conversion_kind, user_pseudonym, touch_ts, event_kind, event_dedup_key)'
    ),
    partition_by='toStartOfMonth(conversion_date)',
    settings={'allow_nullable_key': 1},
    tags=['production', 'mta', 'execution', sector, 'mart'],
    pre_hook=["SET join_algorithm = 'grace_hash'"],
    post_hook=["SET join_algorithm = 'default'"]
) }}

SELECT
    c.user_pseudonym,
    {%- if sector == 'gpay' %}
    c.identity_role,
    {%- endif %}
    c.conversion_kind,
    c.conversion_ts,
    c.conversion_date,
    c.conversion_amount_usd,
    c.conversion_token,
    e.event_ts                                              AS touch_ts,
    e.event_source,
    e.event_kind,
    e.event_subkind,
    e.event_dedup_key,
    dateDiff('second', e.event_ts, c.conversion_ts)         AS lag_seconds,
    dateDiff('day',    toDate(e.event_ts), c.conversion_date) AS lag_days
FROM {{ ref(conversion_model) }} c
INNER JOIN {{ ref(events_model) }} e
    ON  e.user_pseudonym = c.user_pseudonym
    {%- if sector == 'gpay' %}
    AND e.identity_role  = c.identity_role
    {%- endif %}
    AND e.event_ts       <  c.conversion_ts
    AND e.event_ts       >= c.conversion_ts - INTERVAL {{ lookback_days }} DAY
    AND e.event_kind     != {{ conversion_kind_to_event_kind('c.conversion_kind') }}
WHERE 1=1
{%- if start_month and end_month %}
  AND toStartOfMonth(c.conversion_date) >= toDate('{{ start_month }}')
  AND toStartOfMonth(c.conversion_date) <= toDate('{{ end_month }}')
{%- else %}
  {{ apply_monthly_incremental_filter('c.conversion_date', 'conversion_date', add_and=True) }}
{%- endif %}
{% endmacro %}
