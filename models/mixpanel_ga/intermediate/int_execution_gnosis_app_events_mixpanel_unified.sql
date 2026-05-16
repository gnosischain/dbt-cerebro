{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if start_month else 'delete+insert'),
    engine='ReplacingMergeTree()',
    order_by='(event_date, user_pseudonym, event_ts, event_kind, event_dedup_key)',
    unique_key='(event_ts, event_kind, user_pseudonym, event_dedup_key)',
    partition_by='toStartOfMonth(event_date)',
    settings={'allow_nullable_key': 1},
    tags=['production', 'mta', 'mixpanel_ga', 'gnosis_app'],
    pre_hook=["SET join_algorithm = 'grace_hash'"],
    post_hook=["SET join_algorithm = 'default'"]
  )
}}
{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

-- Long-form Mixpanel event log filtered to identified, production traffic
-- and joined to the GA user identity bridge so only events from users in
-- the GA cohort flow through. Anonymous Mixpanel visitors and non-GA
-- identified users are excluded.
--
-- Joins on user_pseudonym = stg_mixpanel_ga__events.user_id_hash, which
-- works because both sides apply the same `pseudonymize_address` macro
-- with the same salt (deterministic).

WITH bridge AS (
    SELECT user_pseudonym
    FROM {{ ref('int_execution_gnosis_app_user_identity_bridge') }}
)

SELECT
    e.event_time                                            AS event_ts,
    e.event_date                                            AS event_date,
    e.user_id_hash                                          AS user_pseudonym,
    'mixpanel'                                              AS event_source,
    concat('mp.', e.event_category)                         AS event_kind,
    e.event_name                                            AS event_subkind,
    CAST(NULL AS Nullable(Float64))                         AS amount_usd,
    cityHash64(e.insert_id)                                 AS event_dedup_key,
    'stg_mixpanel_ga__events'                               AS provenance_model,
    e.device_type                                           AS device_type,
    e.country_code                                          AS country_code,
    e.page_path                                             AS page_path,
    e.bottom_sheet                                          AS bottom_sheet
FROM {{ ref('stg_mixpanel_ga__events') }} e
INNER JOIN bridge b ON b.user_pseudonym = e.user_id_hash
WHERE e.is_production = 1
  AND e.is_identified = 1
  AND e.event_date < today()
{% if start_month and end_month %}
  AND toStartOfMonth(e.event_date) >= toDate('{{ start_month }}')
  AND toStartOfMonth(e.event_date) <= toDate('{{ end_month }}')
{% else %}
  {{ apply_monthly_incremental_filter('e.event_date', 'event_date', add_and=True) }}
{% endif %}
