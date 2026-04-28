

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
-- Dedup rule (v2, 2026-04): ROW_NUMBER() partitioned by
--   (source_pubkey, is_self_consolidation)
-- where is_self_consolidation = (source_pubkey = target_pubkey). A single validator can
-- legitimately have BOTH a self-consolidation (credential switch 0x01→0x02; validator
-- keeps running) AND a later cross-consolidation (balance transfer out; validator exits).
-- Both apply and the beacon chain processes them in sequence. Partitioning dedup by
-- (source, is_self) keeps one row of each kind per source, rather than dropping the
-- cross-consolidation when the self-consolidation happens to be submitted first.
--
-- Observed impact of getting this wrong: on 2025-07-14 validator 113039 (and 65 sibling
-- validators in the same batch) appeared as TARGETS of 33 consolidations each. Their own
-- outbound cross-consolidation request — which is what actually reduced their balance to
-- 0 — was being dropped by the old v1 dedup because a self-consolidation request from
-- the same source had been submitted earlier. Result: 1,056 GNO of phantom inflow per
-- validator, totalling -33,932 GNO of phantom negative income on that single day (out of
-- the model's -31,536 GNO network total).
--
-- Within each (source, is_self) partition, we still keep only the earliest request. Per
-- EIP-7251 the beacon-chain queue processes the first valid request of each kind and
-- rejects subsequent resubmissions.
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
            PARTITION BY
                lower(JSONExtractString(c, 'source_pubkey'))
                ,(lower(JSONExtractString(c, 'source_pubkey'))
                  = lower(JSONExtractString(c, 'target_pubkey')))
            ORDER BY r.slot
        ) AS rn
    FROM `dbt`.`stg_consensus__execution_requests` r
    ARRAY JOIN JSONExtractArrayRaw(payload, 'consolidations') AS c
    WHERE r.slot_timestamp < today()
)
WHERE rn = 1