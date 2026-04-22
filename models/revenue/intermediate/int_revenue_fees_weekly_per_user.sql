{% set lookback_weeks        = 4  %}
{% set window_history_weeks  = 52 %}

{% set all_streams = [
    ('holdings', 'int_revenue_holdings_fees_daily', 'EURe'),
    ('holdings', 'int_revenue_holdings_fees_daily', 'USDC.e'),
    ('holdings', 'int_revenue_holdings_fees_daily', 'BRLA'),
    ('holdings', 'int_revenue_holdings_fees_daily', 'ZCHF'),
    ('sdai',     'int_revenue_sdai_fees_daily',     'sDAI'),
    ('gpay',     'int_revenue_gpay_fees_daily',     'EURe'),
    ('gpay',     'int_revenue_gpay_fees_daily',     'GBPe'),
    ('gpay',     'int_revenue_gpay_fees_daily',     'USDC.e'),
] %}

{# Optional `slice` var (comma-separated) selects which stream_type+symbol
   slices to process in this run. Stages in schema.yml use it so each run
   processes one slice at a time, keeping memory bounded. Format is
   `stream:SYMBOL` pairs; a bare `SYMBOL` matches every stream with that
   symbol. #}
{% set slice_filter = var('slice', none) %}
{% set streams = [] %}
{% if slice_filter %}
  {% set wanted = slice_filter.split(',') | map('trim') | list %}
  {% for s in all_streams %}
    {% set key_full = s[0] ~ ':' ~ s[2] %}
    {% if key_full in wanted or s[2] in wanted %}
      {% do streams.append(s) %}
    {% endif %}
  {% endfor %}
{% else %}
  {% set streams = all_streams %}
{% endif %}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if start_month else 'delete+insert'),
    engine='ReplacingMergeTree()',
    order_by='(week, stream_type, symbol, user)',
    partition_by='toStartOfMonth(week)',
    unique_key='(week, stream_type, symbol, user)',
    settings={'allow_nullable_key': 1},
    tags=['production','revenue','revenue_cross']
  )
}}

{% if is_incremental() and not (start_month and end_month) %}
WITH cutoff AS (
    SELECT coalesce(max(week), toDate('1970-01-01')) - INTERVAL {{ lookback_weeks }} WEEK AS cutoff_week
    FROM {{ this }}
)
{% endif %}

{% for stream_type, source_ref, symbol in streams %}
{% if not loop.first %}
UNION ALL
{% endif %}

SELECT
    week,
    '{{ stream_type }}' AS stream_type,
    symbol,
    user,
    week_fees,
    annual_rolling_fees
FROM (
    WITH weekly_sparse AS (
        SELECT
            toStartOfWeek(date, 1) AS week,
            user,
            symbol,
            round(sum(fees), 8) AS week_fees
        FROM {{ ref(source_ref) }}
        WHERE date < toStartOfWeek(today(), 1)
          AND symbol = '{{ symbol }}'
          {% if is_incremental() and not (start_month and end_month) %}
            AND toStartOfWeek(date, 1) >= (SELECT cutoff_week FROM cutoff) - INTERVAL {{ window_history_weeks }} WEEK
          {% elif start_month and end_month %}
            AND toStartOfWeek(date, 1) >= toDate('{{ start_month }}') - INTERVAL {{ window_history_weeks }} WEEK
            AND date <= addDays(toLastDayOfMonth(toDate('{{ end_month }}')), 6)
          {% endif %}
        GROUP BY week, user, symbol
    ),
    user_span AS (
        SELECT
            user,
            symbol,
            {% if is_incremental() and not (start_month and end_month) %}
              greatest(min(week),
                       (SELECT cutoff_week FROM cutoff) - INTERVAL {{ window_history_weeks }} WEEK) AS first_week,
              toStartOfWeek(today(), 1) - INTERVAL 1 WEEK AS last_week
            {% elif start_month and end_month %}
              greatest(min(week),
                       toDate('{{ start_month }}') - INTERVAL {{ window_history_weeks }} WEEK) AS first_week,
              least(
                toStartOfWeek(today(), 1) - INTERVAL 1 WEEK,
                toStartOfWeek(toLastDayOfMonth(toDate('{{ end_month }}')), 1)
              ) AS last_week
            {% else %}
              min(week) AS first_week,
              toStartOfWeek(today(), 1) - INTERVAL 1 WEEK AS last_week
            {% endif %}
        FROM weekly_sparse
        GROUP BY 1, 2
    ),
    calendar AS (
        SELECT
            user,
            symbol,
            arrayJoin(
                arrayMap(
                    i -> toDate(addWeeks(first_week, i)),
                    range(toUInt32(dateDiff('week', first_week, last_week) + 1))
                )
            ) AS week
        FROM user_span
    ),
    weekly_dense AS (
        SELECT
            c.week,
            c.user,
            c.symbol,
            coalesce(ws.week_fees, 0) AS week_fees
        FROM calendar c
        LEFT JOIN weekly_sparse ws
            ON  ws.week   = c.week
            AND ws.user   = c.user
            AND ws.symbol = c.symbol
    )
    SELECT
        week,
        user,
        symbol,
        week_fees,
        round(
            sum(week_fees) OVER (
                PARTITION BY user
                ORDER BY week
                ROWS BETWEEN 51 PRECEDING AND CURRENT ROW
            ),
            8
        ) AS annual_rolling_fees
    FROM weekly_dense
) s
WHERE (s.annual_rolling_fees > 0 OR s.week_fees > 0)
  {% if is_incremental() and not (start_month and end_month) %}
    AND s.week > (SELECT cutoff_week FROM cutoff)
  {% elif start_month and end_month %}
    AND toStartOfMonth(s.week) >= toDate('{{ start_month }}')
    AND toStartOfMonth(s.week) <= toDate('{{ end_month }}')
  {% endif %}
{% endfor %}
