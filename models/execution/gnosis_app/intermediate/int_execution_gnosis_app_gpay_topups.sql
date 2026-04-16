{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if start_month else 'delete+insert'),
    engine='ReplacingMergeTree()',
    order_by='(block_timestamp, transaction_hash, log_index)',
    unique_key='(transaction_hash, log_index, gp_wallet)',
    partition_by='toStartOfMonth(block_timestamp)',
    settings={'allow_nullable_key': 1},
    pre_hook=["SET join_algorithm = 'grace_hash'"],
    tags=['production','execution','gnosis_app','gpay','topups']
  )
}}

WITH ga_users AS (
    SELECT address FROM {{ ref('int_execution_gnosis_app_users_current') }}
),

-- GA-user CoW trades in the incremental window.
ga_trades AS (
    SELECT
        t.block_number,
        t.block_timestamp,
        t.transaction_hash,
        t.log_index,
        t.taker,
        t.order_uid,
        t.token_bought_address,
        t.token_bought_symbol,
        t.amount_bought,
        t.amount_bought_raw,
        t.token_sold_address,
        t.token_sold_symbol,
        t.amount_sold,
        t.amount_sold_raw,
        t.amount_usd,
        t.solver
    FROM {{ ref('int_execution_cow_trades') }} t
    WHERE t.taker IN (SELECT address FROM ga_users)
      AND t.block_timestamp >= toDateTime('2025-11-12')
      {% if start_month and end_month %}
        AND toStartOfMonth(t.block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(t.block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('t.block_timestamp', 'block_timestamp', add_and=True) }}
      {% endif %}
),

-- GP Safe deposits ("Crypto Deposit" action) in the same window.
-- tx_hash is 0x-prefixed on both sides, so the join is direct.
gp_deposits AS (
    SELECT
        a.transaction_hash                      AS transaction_hash,
        a.wallet_address                        AS gp_wallet,
        a.token_address                         AS token_address,
        a.symbol                                AS token_received_symbol,
        a.amount                                AS amount_received,
        a.amount_usd                            AS amount_received_usd
    FROM {{ ref('int_execution_gpay_activity') }} a
    WHERE a.action = 'Crypto Deposit'
      AND a.block_timestamp >= toDateTime('2025-11-12')
      {% if start_month and end_month %}
        AND toStartOfMonth(a.block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(a.block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('a.block_timestamp', 'block_timestamp', add_and=True) }}
      {% endif %}
)

SELECT
    t.block_number                   AS block_number,
    t.block_timestamp                AS block_timestamp,
    t.transaction_hash               AS transaction_hash,
    t.log_index                      AS log_index,
    t.taker                          AS ga_user,
    d.gp_wallet                      AS gp_wallet,
    t.order_uid                      AS order_uid,
    t.token_sold_address             AS token_sold_address,
    t.token_sold_symbol              AS token_sold_symbol,
    t.amount_sold                    AS amount_sold,
    t.token_bought_address           AS token_bought_address,
    t.token_bought_symbol            AS token_bought_symbol,
    t.amount_bought                  AS amount_bought,
    coalesce(t.amount_usd,
             d.amount_received_usd)  AS amount_usd,
    t.solver                         AS solver
FROM ga_trades t
INNER JOIN gp_deposits d
    ON d.transaction_hash = t.transaction_hash
   AND lower(d.token_address) = lower(t.token_bought_address)
