{{
    config(
        materialized='view',
        tags=['dev', 'execution', 'pools', 'trades', 'api']
    )
}}

-- Daily aggregator market share as % of trades (not hops). A trade is counted
-- against its tx-level aggregator label — the `to_address` matched against
-- known aggregators in int_crawlers_data_labels. Unlabeled (direct) trades
-- are bucketed as "Direct". Result is stacked-100% by day.

WITH

trades AS (
    -- Collapse to one row per transaction (matches the "trade" concept in
    -- the live feed). Use min(tx_to) since it's identical across all hops
    -- of the same tx.
    SELECT
        toDate(block_timestamp)                             AS date,
        transaction_hash,
        min(tx_to)                                          AS to_address
    FROM {{ ref('int_execution_pools_dex_trades') }}
    WHERE block_timestamp < today()
    GROUP BY date, transaction_hash
),

labeled AS (
    SELECT
        t.date,
        t.transaction_hash,
        coalesce(nullIf(lbl.project, ''), 'Direct')         AS aggregator
    FROM trades t
    LEFT JOIN {{ ref('int_crawlers_data_labels') }} lbl
        ON lbl.address = concat('0x', lower(replaceAll(coalesce(t.to_address, ''), '0x', '')))
),

daily_totals AS (
    SELECT date, count() AS total_trades
    FROM labeled
    GROUP BY date
)

SELECT
    l.date                                                  AS date,
    l.aggregator                                            AS label,
    round(100.0 * count() / d.total_trades, 2)              AS value
FROM labeled l
JOIN daily_totals d ON d.date = l.date
GROUP BY l.date, l.aggregator, d.total_trades
ORDER BY date, label
