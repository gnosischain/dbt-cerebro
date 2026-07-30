{{ config(severity='warn', tags=['production', 'data_quality', 'data_quality_daily', 'source_freshness']) }}
-- A run of consecutive block_numbers with ZERO logs (all contracts) between two present
-- blocks = a raw indexer skip — a layer BELOW dbt; no re-decode can recover it.
-- Lesson: raw-logs-ingestion-holes (confirmed 2026-07: blocks 47,089,900-47,089,999
-- dropped 48 WxDAI inflows). On a live chain a >5-block zero-log span is not "quiet
-- blocks". Confirm a flagged range on-chain (eth_getLogs) before requesting a re-index.
WITH recent_blocks AS (
    SELECT DISTINCT block_number
    FROM {{ source('execution', 'logs') }}
    WHERE block_number >= (SELECT max(block_number) - 1200000 FROM {{ source('execution', 'logs') }})
),
gaps AS (
    SELECT block_number AS b,
           leadInFrame(block_number) OVER (ORDER BY block_number ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS nb
    FROM recent_blocks
)
SELECT b + 1 AS gap_start_block, nb - 1 AS gap_end_block, nb - b - 1 AS missing_blocks
FROM gaps
WHERE nb - b > 5
ORDER BY missing_blocks DESC
