-- depends_on: {{ ref('mta_funnels') }}
{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if start_month else 'delete+insert'),
    engine='ReplacingMergeTree()',
    order_by='(date, funnel_name, user_pseudonym)',
    unique_key='(date, funnel_name, user_pseudonym)',
    partition_by='toStartOfMonth(date)',
    settings={'allow_nullable_key': 1},
    tags=['production', 'mta', 'execution', 'gnosis_app', 'mart'],
    pre_hook=["SET join_algorithm = 'grace_hash'"],
    post_hook=["SET join_algorithm = 'default'"]
  )
}}
{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

-- Daily funnel diagnostics over the unified events table. Each funnel is
-- defined as one row in seed `mta_funnels.csv` (funnel_name + 3 step
-- event_kinds + window_seconds) and materialized into one SELECT block
-- per funnel via the Jinja loop below.
--
-- The loop is required because ClickHouse's `windowFunnel` needs
-- `window_seconds` as a compile-time literal — a CROSS JOIN against the
-- seed with `windowFunnel(f.window_seconds)` is rejected as
-- BAD_ARGUMENTS at runtime.
--
-- To add a new funnel: append a row to mta_funnels.csv AND
-- `dbt seed --select mta_funnels`. The next `dbt run` regenerates this
-- model with the new SELECT block automatically.

{% if execute %}
  {% set funnel_query %}
    SELECT
      funnel_name,
      step_1_event_kind,
      step_2_event_kind,
      step_3_event_kind,
      toString(window_seconds) AS window_seconds
    FROM {{ ref('mta_funnels') }}
  {% endset %}
  {% set funnels = run_query(funnel_query).rows %}
{% else %}
  {% set funnels = [] %}
{% endif %}

WITH events AS (
  SELECT
    user_pseudonym,
    event_date,
    event_ts,
    event_kind
  FROM {{ ref('int_execution_gnosis_app_user_events_unified') }}
  WHERE event_date < today()
  {% if start_month and end_month %}
    AND toStartOfMonth(event_date) >= toDate('{{ start_month }}')
    AND toStartOfMonth(event_date) <= toDate('{{ end_month }}')
  {% else %}
    {{ apply_monthly_incremental_filter('event_date', 'date', add_and=True) }}
  {% endif %}
)

{% for f in funnels %}
{% set name           = f[0] %}
{% set step1          = f[1] %}
{% set step2          = f[2] %}
{% set step3          = f[3] %}
{% set window_seconds = f[4] %}
{% if not loop.first %}UNION ALL{% endif %}
SELECT
  e.event_date                                     AS date,
  '{{ name }}'                                     AS funnel_name,
  e.user_pseudonym                                 AS user_pseudonym,
  windowFunnel({{ window_seconds }})(
    toUInt32(toUnixTimestamp(e.event_ts)),
    e.event_kind = '{{ step1 }}',
    e.event_kind = '{{ step2 }}'
    {% if step3 and step3 != '' %}
    , e.event_kind = '{{ step3 }}'
    {% endif %}
  )                                                AS level,
  min(e.event_ts)                                  AS first_event_ts,
  max(e.event_ts)                                  AS last_event_ts
FROM events e
WHERE e.event_kind IN (
  '{{ step1 }}',
  '{{ step2 }}'
  {% if step3 and step3 != '' %}
  , '{{ step3 }}'
  {% endif %}
)
GROUP BY e.event_date, e.user_pseudonym
{% endfor %}
