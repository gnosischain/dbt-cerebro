{% set active_threshold_usd = 0.5 %}
{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

{#
  Same shape as int_revenue_active_users_totals_weekly: cron runs recompute
  complete month partitions via insert_overwrite with the same 2-month
  lookback the upstream (int_revenue_fees_monthly_per_user) uses for its own
  restatement window. Strategy resolves to `append` when start_month is set
  so refresh.py stages append non-overlapping windows instead of REPLACE
  PARTITION wiping earlier stages
  (docs/lessons/staged-insert-overwrite-wipe.md).
#}
{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if start_month else 'insert_overwrite'),
    engine='ReplacingMergeTree()',
    order_by='(month)',
    partition_by='month',
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
        month,
        user,
        sum(month_fees) AS month_fees
    FROM {{ ref('int_revenue_fees_monthly_per_user') }}
    {% if start_month and end_month %}
    WHERE month >= toDate('{{ start_month }}')
      AND month <= toDate('{{ end_month }}')
    {% else %}
    {{ apply_monthly_incremental_filter('month', 'month', false, lookback_days=2, lookback_res='month') }}
    {% endif %}
    GROUP BY month, user
)

SELECT
    month,
    countIf(month_fees >= {{ active_threshold_usd }}) AS users_cnt,
    round(sumIf(month_fees, month_fees >= {{ active_threshold_usd }}), 2) AS fees_total
FROM per_user
GROUP BY month
