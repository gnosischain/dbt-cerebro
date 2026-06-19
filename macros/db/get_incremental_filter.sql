{#
  IMPORTANT — strategy-aware duplicate/data safety. Three branches:

  1. insert_overwrite (partition replace): emits a WHOLE-PARTITION (month-
     granular) filter. dbt-clickhouse replaces every partition present in the
     incremental result; if the filter returned only the last N days the
     replace would wipe the rest of the month. So under insert_overwrite the
     filter must return COMPLETE months (toStartOfMonth lower bound only, no
     day-level restriction) — the recompute is lossless. This branch takes
     PRECEDENCE even when incremental_end_date is set: the microbatch runner's
     single-day end date is applied only as an UPPER cap, never as the lower
     bound. (Letting the microbatch branch below win here is what wiped a whole
     month down to one day under REPLACE PARTITION.)

  2. microbatch append (incremental_end_date set): strict no-overlap
     `> max(date)` ... `<= incr_end`. Exact-once append, no duplicates.

  3. legacy delete+insert (day-granular lookback): re-pulls the last N days;
     delete+insert removes the matching unique_key rows first, so the narrow
     window is safe. NEVER use this branch with `append` (it would duplicate)
     — append models use a strict block_number/date watermark instead.
#}
{% macro apply_monthly_incremental_filter(source_field, destination_field=None, add_and=False, lookback_days=1, lookback_res='day', filters_sql='') %}
  {% if is_incremental() %}
    {% set dest_field = destination_field if destination_field is not none else source_field %}
    {# `price_lookback_days` widens the window for every caller of this macro.
       Used by scripts/maintenance/refill_after_price_gap.sh to recover after a
       prices-source gap: the lineage selector (`int_execution_token_prices_daily+`)
       picks up every consumer transitively, and the var here makes them all
       re-pull the wider window without per-model edits or a tag registry. #}
    {% set effective_lookback = var('price_lookback_days', lookback_days) | int %}
    {% set lb_days = effective_lookback - 1 %}
    {% set incr_end = mb_var('incremental_end_date') %}
    {% set strategy = config.get('incremental_strategy') %}

    {{ "AND " if add_and else "WHERE " }}
    {% if strategy == 'insert_overwrite' %}
      {# Whole-partition recompute: return COMPLETE months so REPLACE PARTITION is
         lossless, EVEN when the microbatch runner set incremental_end_date. A
         day-level lower bound here would make REPLACE PARTITION wipe the rest of
         the month (the June data-loss bug). When incr_end is set we still honor it
         as an upper cap so the runner stays the ceiling authority. #}
      toStartOfMonth(toDate({{ source_field }})) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.{{ dest_field }})), -{{ lb_days }}))
        FROM {{ this }} AS x1
        WHERE 1=1 {{ filters_sql }}
      )
      {% if incr_end is not none %}
      AND toDate({{ source_field }}) <= toDate('{{ incr_end }}')
      {% endif %}
    {% elif incr_end is not none %}
      {# No-overlap microbatch path: strict > max(target_date), <= incr_end.
         Lookback is intentionally suppressed — the microbatch runner is the
         authority on what's been processed. Allowing the macro's lookback
         here would re-pull old days under `append` strategy and create
         duplicates that we'd then need OPTIMIZE FINAL to clean up. #}
      toDate({{ source_field }}) > (
        SELECT coalesce(max(toDate(x1.{{ dest_field }})), toDate('1970-01-01'))
        FROM {{ this }} AS x1
        WHERE 1=1 {{ filters_sql }}
      )
      AND toDate({{ source_field }}) <= toDate('{{ incr_end }}')
    {% else %}
      {# Original lookback path — used when called outside the microbatch runner. #}
      toStartOfMonth(toDate({{ source_field }})) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.{{ dest_field }})), -{{ lb_days }}))
        FROM {{ this }} AS x1
        WHERE 1=1 {{ filters_sql }}
      )
      AND toDate({{ source_field }}) >= (
        SELECT
          {% if lookback_res == 'week' %}
            toStartOfWeek(addDays(max(toDate(x2.{{ dest_field }})), -{{ lb_days }}))
          {% elif lookback_res == 'month' %}
            toStartOfMonth(addDays(max(toDate(x2.{{ dest_field }})), -{{ lb_days }}))
          {% else %}
            addDays(max(toDate(x2.{{ dest_field }})), -{{ lb_days }})
          {% endif %}

        FROM {{ this }} AS x2
        WHERE 1=1 {{ filters_sql }}
      )
    {% endif %}
  {% endif %}
{% endmacro %}
