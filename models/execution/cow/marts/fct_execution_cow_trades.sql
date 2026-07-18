{#
  Batched backfill (scripts/full_refresh/refresh.py) passes start_month, which
  flips the strategy to 'append' so per-month batches are plain INSERTs and never
  touch system.parts (which insert_overwrite's partition-replace needs, and which
  playground/dev users aren't granted). Prod daily runs pass no start_month and
  stay on insert_overwrite. Mirrors the tokens balances models.
#}
{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if var('start_month', none) else 'insert_overwrite'),
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, transaction_hash, log_index)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'cow', 'trades', 'microbatch']
    )
}}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

WITH

trades AS (
    SELECT
        *,
        lower(transaction_hash) AS tx_hash_norm
    FROM {{ ref('int_execution_cow_trades') }}
    {% if start_month and end_month %}
    WHERE toStartOfMonth(block_timestamp) >= toDate('{{ start_month }}')
      AND toStartOfMonth(block_timestamp) <= toDate('{{ end_month }}')
    {% else %}
      {{ apply_monthly_incremental_filter('block_timestamp', 'block_timestamp') }}
    {% endif %}
),

api_fees AS (
    SELECT
        order_uid,
        tx_hash,
        log_index,
        fee_token,
        fee_amount,
        surplus_policy_type,
        surplus_component_raw,
        surplus_factor
    FROM {{ ref('stg_crawlers_data__cow_api_trade_fees') }}
    WHERE order_uid IN (SELECT order_uid FROM trades)
)

SELECT
    t.block_number                                                                   AS block_number,
    t.block_timestamp                                                                AS block_timestamp,
    t.transaction_hash                                                               AS transaction_hash,
    t.log_index                                                                      AS log_index,
    t.protocol                                                                       AS protocol,
    t.pool_address                                                                   AS pool_address,
    t.token_bought_address                                                           AS token_bought_address,
    t.token_bought_symbol                                                            AS token_bought_symbol,
    t.amount_bought_raw                                                              AS amount_bought_raw,
    t.amount_bought                                                                  AS amount_bought,
    t.token_sold_address                                                             AS token_sold_address,
    t.token_sold_symbol                                                              AS token_sold_symbol,
    t.amount_sold_raw                                                                AS amount_sold_raw,
    t.amount_sold                                                                    AS amount_sold,
    t.amount_usd                                                                     AS amount_usd,
    t.fee_amount_raw                                                                 AS fee_amount_raw,
    t.fee_amount                                                                     AS fee_amount,
    f.fee_token                                                                      AS api_fee_token,
    f.fee_amount                                                                     AS api_fee_amount_raw,
    COALESCE(
        CASE WHEN t.fee_amount_raw > 0
             THEN t.amount_usd * toFloat64(t.fee_amount_raw) / nullIf(toFloat64(t.amount_sold_raw), 0)
        END,
        CASE
            WHEN f.fee_token = t.token_sold_address
            THEN t.amount_usd * toFloat64(f.fee_amount) / nullIf(toFloat64(t.amount_sold_raw), 0)
            WHEN f.fee_token = t.token_bought_address
            THEN t.amount_usd * toFloat64(f.fee_amount) / nullIf(toFloat64(t.amount_bought_raw), 0)
        END
    )                                                                                AS fee_usd,
    multiIf(
        t.fee_amount_raw > 0,             'onchain',
        f.fee_token != '',                'api',
        NULL
    )                                                                                AS fee_source,
    -- Total gross value the solver found beyond the reference price (quote for limit
    -- orders, clearing price for market orders), in USD. Equals fee_usd / factor, so
    -- for a 50/50 split: solver_value_usd = 2 * the priceImprovement/surplus component
    -- of fee_usd. NULL for pre-Sep-2024 trades and volume-only fee policies.
    CASE
        WHEN f.surplus_policy_type IN ('priceImprovement', 'surplus')
             AND f.surplus_factor > 0
             AND toFloat64OrZero(f.surplus_component_raw) > 0
        THEN
            CASE
                WHEN f.fee_token = t.token_sold_address
                THEN t.amount_usd
                     * (toFloat64OrZero(f.surplus_component_raw) / f.surplus_factor)
                     / nullIf(toFloat64(t.amount_sold_raw), 0)
                WHEN f.fee_token = t.token_bought_address
                THEN t.amount_usd
                     * (toFloat64OrZero(f.surplus_component_raw) / f.surplus_factor)
                     / nullIf(toFloat64(t.amount_bought_raw), 0)
            END
    END                                                                              AS solver_value_usd,
    t.taker                                                                          AS taker,
    t.order_uid                                                                      AS order_uid,
    t.solver                                                                         AS solver
FROM trades t
-- Join per fill, not per order: partially-fillable orders settle across
-- multiple trades sharing an order_uid, each with its own protocol fees.
-- Joining on order_uid alone attaches the same fee row to every fill and
-- inflates summed fee_usd / solver_value_usd (~12x on multi-fill orders).
-- API txHash/logIndex match the on-chain values exactly (validated Jun 2026).
LEFT JOIN api_fees f
    ON  f.order_uid = t.order_uid
    AND f.tx_hash   = t.tx_hash_norm
    AND f.log_index = t.log_index
