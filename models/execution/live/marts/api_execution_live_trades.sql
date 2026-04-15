{{
    config(
        materialized='view',
        tags=['live', 'execution', 'pools', 'trades', 'api']
    )
}}

{#
    Dashboard feed of recent DEX trades on Gnosis Chain.
    - Window: last 30 minutes of CACHED data, excluding the most recent 60s
      (reorg buffer). Anchored on `max(block_timestamp)` in
      `int_live__dex_trades_raw` — that's the data we've actually materialized,
      which may be a few minutes behind the source HWM depending on the
      incremental refresh cadence. To surface ingestion-level staleness
      separately, query `api_execution_live_trades_freshness`.
    - Multi-hop routes are collapsed to one row per transaction:
        token_sold  = first-hop input   (argMin by log_index)
        token_bought = last-hop output  (argMax by log_index)
        via         = comma-separated list of protocols touched
        hops        = number of swap events in the tx
    - Dust filter: `trade_usd >= live_trades_min_usd` (default 1). Rows with
      unknown USD (no price on either side) are kept; the dashboard can filter
      them if needed.
    - NO LIMIT here. Apply `LIMIT` / ordering tweaks at the dashboard query layer
      so the same model backs multiple views (recent list, big-trade highlights, etc.).
    - `execution_live.transactions` is pre-filtered to the same time window
      BEFORE the join — a full-table join materializes ~10 GB of transactions
      into memory and OOMs downstream aggregates.
#}

{%- set min_usd = var('live_trades_min_usd', 1) -%}

WITH

hwm AS (
    SELECT max(block_timestamp) AS ts
    FROM {{ ref('int_live__dex_trades_raw') }}
),

recent AS (
    SELECT *
    FROM {{ ref('int_live__dex_trades_raw') }}
    WHERE block_timestamp >= (SELECT ts FROM hwm) - INTERVAL 30 MINUTE
      AND block_timestamp <= (SELECT ts FROM hwm) - INTERVAL 60 SECOND
),

recent_tx AS (
    SELECT
        transaction_hash,
        block_number,
        from_address,
        to_address
    FROM {{ source('execution_live', 'transactions') }}
    WHERE block_timestamp >= (SELECT ts FROM hwm) - INTERVAL 30 MINUTE
      AND block_timestamp <= (SELECT ts FROM hwm) - INTERVAL 60 SECOND
),

tx_summary AS (
    SELECT
        transaction_hash,
        min(block_timestamp)                                                AS block_timestamp,
        min(block_number)                                                   AS block_number,
        arrayStringConcat(
            arrayFilter(x -> x != '', groupUniqArray(protocol)), ', '
        )                                                                   AS via,
        count()                                                             AS hops,
        argMin(token_sold_symbol,   log_index)                              AS token_sold,
        argMin(amount_sold,         log_index)                              AS amount_sold,
        argMax(token_bought_symbol, log_index)                              AS token_bought,
        argMax(amount_bought,       log_index)                              AS amount_bought,
        max(amount_usd)                                                     AS trade_usd
    FROM recent
    GROUP BY transaction_hash
)

{#
    Explicit AS on EVERY column. The ClickHouse new analyzer otherwise keeps
    the CTE alias (e.g. "s.transaction_hash") in the output column name when
    there's a collision with another CTE — which breaks downstream callers
    that expect bare `transaction_hash`. This also used to trip up explicit
    column lists in SELECTs from this view with "Unknown expression identifier"
    errors; aliasing here fixes both.
#}
SELECT
    s.block_timestamp            AS block_timestamp,
    s.block_number               AS block_number,
    s.transaction_hash           AS transaction_hash,
    s.token_sold                 AS token_sold,
    round(s.amount_sold, 6)      AS amount_sold,
    s.token_bought               AS token_bought,
    round(s.amount_bought, 6)    AS amount_bought,
    round(s.trade_usd, 2)        AS trade_usd,
    s.via                        AS via,
    s.hops                       AS hops,
    tx.from_address              AS trader,
    lbl.project                  AS aggregator
FROM tx_summary s
LEFT JOIN recent_tx tx
    ON tx.transaction_hash = s.transaction_hash
    AND tx.block_number    = s.block_number
LEFT JOIN {{ ref('int_crawlers_data_labels') }} lbl
    ON lbl.address = concat('0x', lower(replaceAll(coalesce(tx.to_address, ''), '0x', '')))
WHERE (s.token_sold != '' OR s.token_bought != '')
  AND (s.trade_usd IS NULL OR s.trade_usd >= {{ min_usd }})
ORDER BY s.block_timestamp DESC
