{{
  config(
    materialized='view',
    tags=['production','staging','governance']
  )
}}

-- One row per (tx_hash, log_index) -- each on-chain event fires exactly
-- once, so FINAL alone is normally sufficient; the row_number is a defensive
-- backstop against accidental duplicate re-ingestion (ReplacingMergeTree
-- dedup happens on merge, not insert).
SELECT
    tx_hash,
    block_number,
    log_index,
    block_time,
    action,
    delegator,
    delegate
FROM (
    SELECT
        tx_hash, block_number, log_index, block_time, action, delegator, delegate,
        row_number() OVER (
            PARTITION BY tx_hash, log_index
            ORDER BY ingested_at DESC
        ) AS rn
    FROM {{ source('governance', 'snapshot_delegations') }} FINAL
)
WHERE rn = 1
