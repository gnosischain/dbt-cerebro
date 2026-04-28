{% set start_month = var('start_month', none) %}
{% set end_month = var('end_month', none) %}

{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if start_month else 'delete+insert'),
        engine='ReplacingMergeTree()',
        order_by='(date)',
        unique_key='(date)',
        partition_by='toStartOfMonth(date)',
        tags=["production", "consensus", "attestations_performance"]
    )
}}

SELECT
    date
    ,SUM(cnt) AS attestations_total
    ,SUM(inclusion_delay * cnt) / SUM(cnt) AS avg_inclusion_delay
    ,quantileExactWeighted(0.5)(inclusion_delay, cnt) AS p50_inclusion_delay
    ,SUMIf(cnt, inclusion_delay = 1) / SUM(cnt) AS pct_inclusion_distance_1
    ,SUMIf(cnt, inclusion_delay <= 2) / SUM(cnt) AS pct_inclusion_distance_le_2
    ,SUMIf(cnt, inclusion_delay > 1) / SUM(cnt) AS pct_inclusion_distance_gt_1
FROM {{ ref('int_consensus_attestations_daily') }}
WHERE 1=1
{% if start_month and end_month %}
    AND toStartOfMonth(date) >= toDate('{{ start_month }}')
    AND toStartOfMonth(date) <= toDate('{{ end_month }}')
{% else %}
    {{ apply_monthly_incremental_filter('date', 'date', 'true', lookback_days=2) }}
{% endif %}
GROUP BY 1
