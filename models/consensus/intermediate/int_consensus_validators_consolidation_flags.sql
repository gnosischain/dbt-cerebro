{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='(validator_index)',
        tags=["production", "consensus", "validators_consolidations"]
    )
}}

-- Small (~136k × 2 rows max) per-validator lookup: does this validator appear as SOURCE
-- or TARGET in any cross-consolidation request (ever). Materialised once so that
-- downstream per-batch income_daily queries don't have to re-execute the
-- `consolidation_requests JOIN status_latest` hash on every monthly batch (which hits
-- the 10.8 GiB cluster memory cap on the shared tier).
--
-- Self-consolidations (source_pubkey == target_pubkey; credential switch only) are
-- excluded since they don't affect balance attribution; income_daily wants the
-- cross-consolidation signal only.

WITH cross_req AS (
    SELECT source_pubkey, target_pubkey
    FROM {{ ref('int_consensus_validators_consolidation_requests') }}
    WHERE source_pubkey != target_pubkey
),
source_pks AS (SELECT DISTINCT source_pubkey AS pk FROM cross_req),
target_pks AS (SELECT DISTINCT target_pubkey AS pk FROM cross_req)

SELECT
    s.validator_index
    ,if(s.pubkey IN (SELECT pk FROM source_pks), 1, 0) AS has_source_consolidation_request
    ,if(s.pubkey IN (SELECT pk FROM target_pks), 1, 0) AS has_target_consolidation_request
FROM (
    SELECT validator_index, lower(pubkey) AS pubkey
    FROM {{ ref('fct_consensus_validators_status_latest') }}
) s
WHERE s.pubkey IN (SELECT pk FROM source_pks)
   OR s.pubkey IN (SELECT pk FROM target_pks)
