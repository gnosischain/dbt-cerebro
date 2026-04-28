{#
  incremental_strategy resolves to `append` when either start_month
  (full-refresh batching) OR incremental_end_date (microbatch runner) is set.
  Both bound the slice via WHERE clauses; ReplacingMergeTree dedups on
  (block_timestamp, transaction_hash). Eliminates ALTER ... DELETE mutations
  on the daily path.
#}
{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if (var('start_month', none) or var('incremental_end_date', none)) else 'delete+insert'),
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, transaction_hash)',
        unique_key='(block_timestamp, transaction_hash)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={'allow_nullable_key': 1},
        tags=['dev', 'execution', 'pools', 'trades', 'intermediate', 'microbatch']
    )
}}

-- Transaction-grain collapse of int_execution_pools_dex_trades. One row per
-- (date, transaction_hash). Does the expensive tx-level GROUP BY and the
-- aggregator-label JOIN exactly once, here. Downstream facts and api views
-- read a pre-shaped table instead of re-collapsing 31M+ swap rows.
--
-- aggregator_label uses the Option 2 heuristic:
--   labeled in int_crawlers_data_labels → project name
--   unlabeled + hop_count >= 2         → 'Other Router' (definitely routed)
--   unlabeled + hop_count = 1          → 'Direct'       (conservative)
--
-- hop_bucket and size_bucket are pre-computed so downstream api views are
-- trivial GROUP BYs on string columns, not multiIf expressions.

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

WITH

swaps AS (
    SELECT *
    FROM {{ ref('int_execution_pools_dex_trades') }}
    {% if start_month and end_month %}
    WHERE toStartOfMonth(block_timestamp) >= toDate('{{ start_month }}')
      AND toStartOfMonth(block_timestamp) <= toDate('{{ end_month }}')
    {% else %}
      {{ apply_monthly_incremental_filter('block_timestamp', 'block_timestamp') }}
    {% endif %}
),

tx_collapsed AS (
    SELECT
        transaction_hash,
        min(block_timestamp)                                                    AS block_timestamp,
        any(tx_from)                                                            AS tx_from,
        any(tx_to)                                                              AS tx_to,
        count()                                                                 AS hop_count,
        argMin(token_sold_symbol,   log_index)                                  AS token_sold_first,
        argMax(token_bought_symbol, log_index)                                  AS token_bought_last,
        max(amount_usd)                                                         AS trade_usd,
        arrayDistinct(
            arrayMap(
                t -> t.2,
                arraySort(groupArray(tuple(log_index, protocol)))
            )
        )                                                                       AS protocols_used
    FROM swaps
    GROUP BY transaction_hash
),

labeled AS (
    SELECT
        s.*,
        nullIf(lbl.project, '') AS project_label
    FROM tx_collapsed s
    LEFT JOIN {{ ref('int_crawlers_data_labels') }} lbl
        ON lbl.address = concat('0x', lower(replaceAll(coalesce(s.tx_to, ''), '0x', '')))
)

SELECT
    toDate(block_timestamp)                                                     AS date,
    block_timestamp,
    transaction_hash,
    tx_from,
    tx_to,
    multiIf(
        project_label IS NOT NULL, project_label,
        hop_count >= 2,            'Other Router',
                                   'Direct'
    )                                                                           AS aggregator_label,
    hop_count,
    multiIf(
        hop_count = 1, '1 hop',
        hop_count = 2, '2 hops',
        hop_count = 3, '3 hops',
                       '4+ hops'
    )                                                                           AS hop_bucket,
    trade_usd,
    multiIf(
        trade_usd IS NULL,   'unknown',
        trade_usd <    100,  '< $100',
        trade_usd <   1000,  '$100 – $1K',
        trade_usd <  10000,  '$1K – $10K',
        trade_usd < 100000,  '$10K – $100K',
                             '$100K+'
    )                                                                           AS size_bucket,
    token_sold_first,
    token_bought_last,
    protocols_used
FROM labeled
