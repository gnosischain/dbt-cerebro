{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date)',
        unique_key='(date)',
        partition_by='toStartOfMonth(date)',
        tags=['execution', 'cow', 'daily']
    )
}}

WITH

trade_daily AS (
    SELECT
        toDate(block_timestamp)                                                      AS date,
        count(*)                                                                     AS num_trades,
        countDistinct(taker)                                                         AS unique_traders,
        sum(amount_usd)                                                              AS volume_usd,
        sumIf(fee_usd, fee_source = 'api')                                           AS fees_usd,
        sumIf(solver_value_usd, fee_source = 'api')                                 AS solver_value_usd
    FROM {{ ref('fct_execution_cow_trades') }}
    {% if is_incremental() %}
    WHERE toStartOfMonth(toDate(block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(date), -3)) FROM {{ this }}
    )
    {% endif %}
    GROUP BY date
),

batch_daily AS (
    SELECT
        toDate(block_timestamp)                                                      AS date,
        count(*)                                                                     AS num_batches,
        countIf(is_cow)                                                              AS num_cow_batches,
        sum(num_interactions)                                                        AS total_interactions,
        sum(tx_cost_native)                                                          AS total_tx_cost_native,
        countDistinct(solver)                                                        AS active_solvers
    FROM {{ ref('int_execution_cow_batches') }}
    {% if is_incremental() %}
    WHERE toStartOfMonth(toDate(block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(date), -3)) FROM {{ this }}
    )
    {% endif %}
    GROUP BY date
)

SELECT
    t.date,
    t.num_trades,
    t.unique_traders,
    t.volume_usd,
    t.fees_usd,
    t.solver_value_usd,
    b.num_batches,
    b.num_cow_batches,
    if(b.num_batches > 0, toFloat64(b.num_cow_batches) / b.num_batches, 0)           AS cow_ratio,
    b.total_interactions,
    b.total_tx_cost_native,
    b.active_solvers
FROM trade_daily t
LEFT JOIN batch_daily b
    ON b.date = t.date
WHERE t.date < today()
ORDER BY t.date
