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
    {% set incr_end = var('incremental_end_date', none) %}

    {{ "AND " if add_and else "WHERE " }}
    {% if incr_end is not none %}
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
