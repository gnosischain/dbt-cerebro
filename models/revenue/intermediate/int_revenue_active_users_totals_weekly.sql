{% set active_threshold_usd = 6.0 %}
{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

{#
  Heavy aggregation (GROUP BY week, user over the 10M+-row per-user weekly
  table) OOMs as a view/table. Cron runs recompute only complete month
  partitions via insert_overwrite (upstream restates the trailing 4 weeks;
  the 35-day lookback covers it; partition grain == overwrite grain). The
  strategy resolves to `append` when start_month is set: refresh.py stages
  write non-overlapping month windows, and insert_overwrite would make each
  stage's REPLACE PARTITION wipe the previous ones
  (docs/lessons/staged-insert-overwrite-wipe.md).
#}
{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if start_month else 'insert_overwrite'),
    engine='ReplacingMergeTree()',
    order_by='(week)',
    partition_by='toStartOfMonth(week)',
    pre_hook=[
        "SET max_bytes_before_external_group_by = 2000000000",
        "SET max_bytes_before_external_sort = 2000000000"
    ],
    post_hook=[
        "SET max_bytes_before_external_group_by = 0",
        "SET max_bytes_before_external_sort = 0"
    ],
    tags=['production','revenue','revenue_cross']
  )
}}

WITH per_user AS (
    SELECT
        week,
        user,
        sum(annual_rolling_fees) AS annual_rolling_fees
    FROM {{ ref('int_revenue_fees_weekly_per_user') }}
    {% if start_month and end_month %}
    WHERE toStartOfMonth(week) >= toDate('{{ start_month }}')
      AND toStartOfMonth(week) <= toDate('{{ end_month }}')
    {% else %}
    {{ apply_monthly_incremental_filter('week', 'week', false, lookback_days=35) }}
    {% endif %}
    GROUP BY week, user
)

SELECT
    week,
    countIf(annual_rolling_fees >= {{ active_threshold_usd }}) AS users_cnt,
    round(sumIf(annual_rolling_fees, annual_rolling_fees >= {{ active_threshold_usd }}), 2) AS annual_rolling_fees_total
FROM per_user
GROUP BY week
