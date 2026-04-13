{{
    config(
        materialized='table',
        engine='ReplacingMergeTree()',
        order_by='(date, solver)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'cow', 'solvers', 'daily']
    )
}}

WITH

solver_trades AS (
    SELECT
        toDate(block_timestamp)                                                      AS date,
        solver,
        count(*)                                                                     AS num_trades,
        countDistinct(taker)                                                         AS unique_traders,
        sum(amount_usd)                                                              AS volume_usd,
        sum(fee_usd)                                                                 AS fees_usd
    FROM {{ ref('fct_execution_cow_trades') }}
    WHERE solver IS NOT NULL
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
