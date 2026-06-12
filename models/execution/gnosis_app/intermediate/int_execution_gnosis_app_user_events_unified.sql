{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    engine='ReplacingMergeTree()',
    order_by='(event_date, event_kind, user_pseudonym, event_ts, event_dedup_key)',
    partition_by='toStartOfMonth(event_date)',
    settings={'allow_nullable_key': 1},
    tags=['production', 'mta', 'execution', 'gnosis_app']
  )
}}
{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

-- Thin UNION ALL of the chain-side and Mixpanel-side unified event logs.
-- The single touchpoint table the MTA persona's runtime mapping points
-- at as `touchpoint_model`. Reads from already-incremental upstream
-- intermediates, so this model is a low-cost reshuffle.
--
-- Chain rows have NULL for Mixpanel-only columns (device_type,
-- country_code, page_path, bottom_sheet); Mixpanel rows have NULL for
-- amount_usd. event_kind drives all attribution downstream.

SELECT
    event_ts,
    event_date,
    user_pseudonym,
    event_source,
    event_kind,
    event_subkind,
    amount_usd,
    event_dedup_key,
    provenance_model,
    CAST(NULL AS Nullable(String)) AS device_type,
    CAST(NULL AS Nullable(String)) AS country_code,
    CAST(NULL AS Nullable(String)) AS page_path,
    CAST(NULL AS Nullable(String)) AS bottom_sheet
FROM {{ ref('int_execution_gnosis_app_events_chain_unified') }}
WHERE 1=1
{% if start_month and end_month %}
  AND toStartOfMonth(event_date) >= toDate('{{ start_month }}')
  AND toStartOfMonth(event_date) <= toDate('{{ end_month }}')
{% else %}
  {{ apply_monthly_incremental_filter('event_date', 'event_date', add_and=True) }}
{% endif %}

UNION ALL

SELECT
    event_ts,
    event_date,
    user_pseudonym,
    event_source,
    event_kind,
    event_subkind,
    amount_usd,
    event_dedup_key,
    provenance_model,
    device_type,
    country_code,
    page_path,
    bottom_sheet
FROM {{ ref('int_execution_gnosis_app_events_mixpanel_unified') }}
WHERE 1=1
{% if start_month and end_month %}
  AND toStartOfMonth(event_date) >= toDate('{{ start_month }}')
  AND toStartOfMonth(event_date) <= toDate('{{ end_month }}')
{% else %}
  {{ apply_monthly_incremental_filter('event_date', 'event_date', add_and=True) }}
{% endif %}
