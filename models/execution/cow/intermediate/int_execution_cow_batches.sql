{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, transaction_hash)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'cow', 'batches', 'intermediate']
    )
}}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

WITH

trades AS (
    SELECT
        block_timestamp,
        transaction_hash,
        amount_usd,
        solver
    FROM {{ ref('int_execution_cow_trades') }}
    {% if start_month and end_month %}
    WHERE toStartOfMonth(block_timestamp) >= toDate('{{ start_month }}')
      AND toStartOfMonth(block_timestamp) <= toDate('{{ end_month }}')
    {% else %}
      {{ apply_monthly_incremental_filter('block_timestamp', 'block_timestamp') }}
    {% endif %}
),

interactions AS (
    SELECT
        transaction_hash,
        count(*) AS num_interactions
    FROM {{ ref('stg_cow__interactions') }}
    {% if start_month and end_month %}
    WHERE toStartOfMonth(block_timestamp) >= toDate('{{ start_month }}')
      AND toStartOfMonth(block_timestamp) <= toDate('{{ end_month }}')
    {% else %}
      {# Match the trades CTE's whole-month window: under insert_overwrite the
         month is rebuilt in full, so the interactions join must cover the same
         months or pre-(max-3) batches get num_interactions=0 and spurious is_cow. #}
      {{ apply_monthly_incremental_filter('block_timestamp', 'block_timestamp') }}
    {% endif %}
    GROUP BY transaction_hash
),

batch_trades AS (
    SELECT
        min(block_timestamp)                                                         AS block_timestamp,
        transaction_hash,
        any(solver)                                                                  AS solver,
        count(*)                                                                     AS num_trades,
        countIf(isNotNull(amount_usd))                                               AS num_priced_trades,
        sum(amount_usd)                                                              AS batch_value_usd
    FROM trades
    GROUP BY transaction_hash
),

tx_context AS (
    SELECT
        concat('0x', transaction_hash) AS transaction_hash,
        gas_used,
        gas_price
    FROM {{ source('execution', 'transactions') }}
    WHERE replaceAll(lower(to_address), '0x', '') = '9008d19f58aabd9ed0d60971565aa8510560ab41'
    {% if start_month and end_month %}
      AND block_timestamp >= toDate('{{ start_month }}') - INTERVAL 1 DAY
      AND block_timestamp <= toDate('{{ end_month }}') + INTERVAL 32 DAY
    {% else %}
      {# Match the trades CTE's whole-month window so gas/tx_cost is present for
         the full rebuilt month, not just the last 3 days. #}
      {{ apply_monthly_incremental_filter('block_timestamp', 'block_timestamp', add_and=True) }}
    {% endif %}
)

SELECT
    bt.block_timestamp                                                               AS block_timestamp,
    bt.transaction_hash                                                              AS transaction_hash,
    bt.solver                                                                        AS solver,
    bt.num_trades                                                                    AS num_trades,
    coalesce(i.num_interactions, 0)                                                  AS num_interactions,
    coalesce(i.num_interactions, 0) = 0 AND bt.num_trades > 1                        AS is_cow,
    bt.batch_value_usd                                                               AS batch_value_usd,
    tx.gas_used                                                                      AS gas_used,
    tx.gas_price                                                                     AS gas_price,
    toFloat64(tx.gas_used) * toFloat64(tx.gas_price) / 1e18                          AS tx_cost_native
FROM batch_trades bt
LEFT JOIN interactions i
    ON concat('0x', i.transaction_hash) = bt.transaction_hash
LEFT JOIN tx_context tx
    ON tx.transaction_hash = bt.transaction_hash
