{#
  mb_var(name, default) — microbatch-friendly variable resolver.

  Resolution order:
    1. dbt `--vars` (so `refresh.py` historical backfills keep working unchanged)
    2. env var `DBT_MB_<NAME_UPPER>` (set by scripts/refresh/dbt_incremental_runner.py)
    3. `default`

  Why: dbt treats ANY `--vars` change as a global manifest invalidation -> full
  re-parse of the whole project on every microbatch slice (~20s each, the bulk of
  the cron wall-clock). Env vars used in model/macro SQL are tracked per-file by
  dbt's partial parser, so changing `DBT_MB_*` between slices re-parses only the
  files that reference it (~5s, partial-parse speed). The daily microbatch runner
  therefore passes the slice date / stage vars via `DBT_MB_*` instead of `--vars`.
#}
{% macro mb_var(name, default=none) %}
  {%- set _v = var(name, none) -%}
  {%- if _v is not none -%}
    {{- return(_v) -}}
  {%- endif -%}
  {%- set _e = env_var('DBT_MB_' ~ (name | upper), '') -%}
  {{- return(_e if _e != '' else default) -}}
{% endmacro %}
