{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}
{% set incr_end    = var('incremental_end_date', none) %}

{#
  incremental_strategy resolves to `append` when either start_month
  (full-refresh batching) OR incremental_end_date (microbatch runner) is set.
  ReplacingMergeTree dedups on (token_address, date). The runner's
  no-overlap slicing guarantees no duplicate keys are produced on the daily
  path, so ALTER ... DELETE mutations are eliminated.
#}
{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if (start_month or incr_end) else 'delete+insert'),
        engine='ReplacingMergeTree()',
        order_by='(token_address, date)',
        unique_key='(token_address, date)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles_v2', 'supply_daily', 'microbatch']
    )
}}

-- Per-token daily Circles v2 supply, derived from the zero-address balance
-- in int_execution_circles_v2_balances_daily. Built as `int_` so it can run
-- incrementally; the `fct_` mart is a thin view over this table.

WITH balances AS (
    SELECT *
    FROM {{ ref('int_execution_circles_v2_balances_daily') }}
    WHERE account = '0x0000000000000000000000000000000000000000'
    {% if start_month and end_month %}
      AND toStartOfMonth(date) >= toDate('{{ start_month }}')
      AND toStartOfMonth(date) <= toDate('{{ end_month }}')
    {% else %}
      {{ apply_monthly_incremental_filter('date', 'date', add_and=true, lookback_days=2) }}
    {% endif %}
)
SELECT
    date,
    token_address,
    -balance_raw AS supply_raw,
    -balance_raw / POWER(10, 18) AS supply,
    -demurraged_balance_raw / POWER(10, 18) AS demurraged_supply
FROM balances
