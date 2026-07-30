{#
  'append' under batched backfill (start_month set) so per-month batches are plain
  INSERTs and avoid the system.parts grant insert_overwrite needs; insert_overwrite
  for prod daily runs. See fct_execution_cow_trades.
#}
{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if var('start_month', none) else 'insert_overwrite'),
        engine='ReplacingMergeTree()',
        order_by='(date, solver)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'cow', 'solvers', 'daily', 'microbatch']
    )
}}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

WITH

solver_trades AS (
    SELECT
        toDate(block_timestamp)                                                      AS date,
        solver,
        count(*)                                                                     AS num_trades,
        countDistinct(taker)                                                         AS unique_traders,
        sum(amount_usd)                                                              AS volume_usd,
        sumIf(fee_usd, fee_source = 'api')                                           AS fees_usd
    FROM {{ ref('fct_execution_cow_trades') }}
    WHERE solver IS NOT NULL
    {% if start_month and end_month %}
      AND toStartOfMonth(toDate(block_timestamp)) >= toDate('{{ start_month }}')
      AND toStartOfMonth(toDate(block_timestamp)) <= toDate('{{ end_month }}')
    {% elif is_incremental() %}
      AND toStartOfMonth(toDate(block_timestamp)) >= (
          SELECT toStartOfMonth(addDays(max(date), -3)) FROM {{ this }}
      )
    {% endif %}
    GROUP BY date, solver
),

solver_batches AS (
    SELECT
        toDate(block_timestamp)                                                      AS date,
        solver,
        count(*)                                                                     AS num_batches,
        countIf(is_cow)                                                              AS num_cow_batches,
        sum(tx_cost_native)                                                          AS total_tx_cost_native
    FROM {{ ref('int_execution_cow_batches') }}
    WHERE solver IS NOT NULL
    {% if start_month and end_month %}
      AND toStartOfMonth(toDate(block_timestamp)) >= toDate('{{ start_month }}')
      AND toStartOfMonth(toDate(block_timestamp)) <= toDate('{{ end_month }}')
    {% elif is_incremental() %}
      AND toStartOfMonth(toDate(block_timestamp)) >= (
          SELECT toStartOfMonth(addDays(max(date), -3)) FROM {{ this }}
      )
    {% endif %}
    GROUP BY date, solver
)

SELECT
    t.date                                                                           AS date,
    t.solver                                                                         AS solver,
    s.is_active                                                                      AS solver_is_active,
    t.num_trades                                                                     AS num_trades,
    t.unique_traders                                                                 AS unique_traders,
    t.volume_usd                                                                     AS volume_usd,
    t.fees_usd                                                                       AS fees_usd,
    b.num_batches                                                                    AS num_batches,
    b.num_cow_batches                                                                AS num_cow_batches,
    b.total_tx_cost_native                                                           AS total_tx_cost_native
FROM solver_trades t
LEFT JOIN solver_batches b
    ON  b.date   = t.date
    AND b.solver = t.solver
LEFT JOIN {{ ref('fct_execution_cow_solvers') }} s
    ON  s.solver = t.solver
WHERE t.date < today()
ORDER BY t.date, t.volume_usd DESC
