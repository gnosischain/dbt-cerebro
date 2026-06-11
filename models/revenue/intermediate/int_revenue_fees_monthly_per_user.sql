{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

{# Partition grain MUST equal the insert_overwrite grain. This model is
   rebuilt in monthly windows; the previous toStartOfYear(month) partition
   made every incremental run replace the WHOLE year partition with only
   the lookback months, silently deleting all other months of that year
   (observed 2026-06: only Apr-May 2026 and Oct-Dec of prior years
   survived). Monthly partitions stay far below the ClickHouse Cloud
   100-partitions-per-insert cap even on a full-history rebuild. #}
{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    engine='ReplacingMergeTree()',
    order_by='(month, stream_type, symbol, user)',
    partition_by='month',
    settings={'allow_nullable_key': 1},
    tags=['production','revenue','revenue_cross']
  )
}}

-- Reads the unified view (single canonicalization junction for the June
-- 2026 Safe migration) instead of re-unioning the stream models, so the
-- per-user key here always matches the cross-stream canonical address.
WITH daily AS (
    SELECT stream_type, date, user, symbol, fees
    FROM {{ ref('int_revenue_fees_unified_daily') }}
)

SELECT
    toStartOfMonth(date) AS month,
    stream_type,
    user,
    symbol,
    round(sum(fees), 8) AS month_fees
FROM daily
WHERE toStartOfMonth(date) < toStartOfMonth(today())
  {% if start_month and end_month %}
    AND toStartOfMonth(date) >= toDate('{{ start_month }}')
    AND toStartOfMonth(date) <= toDate('{{ end_month }}')
  {% else %}
    {{ apply_monthly_incremental_filter('date', 'month', true, lookback_days=2, lookback_res='month') }}
  {% endif %}
GROUP BY month, stream_type, user, symbol
