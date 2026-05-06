{#
  weekly_spine(start_date_expr, end_date_expr)

  Emits a continuous weekly grid (one row per Monday-week) from
  start_date_expr through end_date_expr (inclusive, snapped to
  toStartOfWeek(_, 1) — Monday start, matching the existing weekly fct
  models like fct_execution_gnosis_app_swaps_weekly).

  Used by MMM intermediates so every (week, kpi_name) / (week, media_name)
  / (week, control_name) row exists even when no underlying data was
  emitted that week. Callers cross-join this spine with a registry seed
  to fill missing weeks with zeros (counts/sums) or NULL (snapshots).

  Pattern (callsite):

    WITH spine AS ({{ weekly_spine("today() - INTERVAL 730 DAY",
                                   "today() - INTERVAL 7 DAY") }})
    SELECT s.week, k.kpi_name, ...
    FROM spine s
    CROSS JOIN {{ ref('mmm_kpi_registry') }} k
    LEFT JOIN <facts> f
      ON  s.week = toStartOfWeek(f.<date_col>, 1)
      AND k.source_model = '<source>'
#}
{% macro weekly_spine(start_date_expr, end_date_expr) %}
SELECT
  toStartOfWeek(toDate({{ start_date_expr }}), 1)
    + toIntervalWeek(n) AS week
FROM (
  SELECT arrayJoin(
    range(0, toUInt32(dateDiff(
      'week',
      toStartOfWeek(toDate({{ start_date_expr }}), 1),
      toStartOfWeek(toDate({{ end_date_expr }}),   1)
    )) + 1)
  ) AS n
)
{% endmacro %}
