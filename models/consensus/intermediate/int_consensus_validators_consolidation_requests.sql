{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='(request_slot)',
        tags=["production", "consensus", "validators_consolidations"]
    )
}}

-- DEDUPED EIP-7251 consolidation requests: one row per unique source_pubkey, keeping
-- the earliest submitted (request_slot = min).
--
-- Why this model exists: per EIP-7251 the beacon chain consolidation queue processes
-- the FIRST valid request per source; subsequent requests for an already-enqueued or
-- already-processed source are rejected. The raw
-- consensus.execution_requests.payload[].consolidations array records every submitted
-- request — including operator resubmissions that never actually applied. On heavy
-- consolidation days in 2025 this inflated target consolidation_inflow_gno by up to
-- 8× (observed: 2025-10-06 target 548367, 449 raw requests vs 109 unique sources,
-- cascading into -36,311 GNO phantom network income).
--
-- Dedup rule: ROW_NUMBER() partitioned by source_pubkey, ordered by request_slot, keep
-- rn=1. This mirrors the beacon chain's slot-FIFO processing. A small residual
-- edge case (~4% of duplicated sources whose earliest request targeted validator A
-- but whose actually-processed request targeted B) remains but covers >95% of
-- cases and all large-impact days.
--
-- Scale: full source `consensus.execution_requests` has ~82k rows; after ARRAY JOIN
-- on the consolidations subarray and dedup we expect a few thousand rows — trivial
-- to fully rebuild. Having a dedicated small "source of truth" model also lets the
-- downstream int_consensus_validators_consolidations_daily keep its monthly-batched
-- snapshot-join pattern (that model OOMs if it tries to dedup AND do the full-history
-- snapshot join in one pass).

SELECT request_slot, request_date, source_pubkey, target_pubkey
FROM (
    SELECT
        r.slot AS request_slot
        ,toStartOfDay(r.slot_timestamp) AS request_date
        ,lower(JSONExtractString(c, 'source_pubkey')) AS source_pubkey
        ,lower(JSONExtractString(c, 'target_pubkey')) AS target_pubkey
        ,ROW_NUMBER() OVER (
            PARTITION BY lower(JSONExtractString(c, 'source_pubkey'))
            ORDER BY r.slot
        ) AS rn
    FROM {{ ref('stg_consensus__execution_requests') }} r
    ARRAY JOIN JSONExtractArrayRaw(payload, 'consolidations') AS c
    WHERE r.slot_timestamp < today()
)
WHERE rn = 1
