{{
  config(
    materialized='view',
    tags=['production','staging','rpc_state_indexer']
  )
}}

WITH anchors AS (
    SELECT
        chain_id,
        snapshot_date,
        argMax(block_number, tuple(resolved_at, toString(resolution_id))) AS anchor_block,
        argMax(block_hash, tuple(resolved_at, toString(resolution_id))) AS anchor_hash
    FROM {{ source('rpc_state_indexer', 'day_anchors') }}
    GROUP BY chain_id, snapshot_date
    HAVING uniqExact(tuple(
               block_number, block_hash, parent_hash, block_timestamp,
               next_block_number, next_block_hash, next_block_timestamp
           )) = 1
       AND min(finalized_at_resolution) = 1
),

attempts AS (
    SELECT
        chain_id,
        job_name,
        target_kind,
        target_address,
        snapshot_date,
        attempt_id,
        integrity_mode,
        anchor_block,
        anchor_hash,
        universe_hash,
        universe_size,
        result_digest,
        batches_total,
        observations_ok
    FROM (SELECT * FROM {{ source('rpc_state_indexer', 'census_attempts') }} FINAL)
    WHERE status = 'verified'
      AND batches_total = batches_verified
      AND observations_failed = 0
),

errored AS (
    SELECT DISTINCT attempt_id
    FROM (SELECT * FROM {{ source('rpc_state_indexer', 'census_errors') }} FINAL)
),

verified AS (
    SELECT
        p.chain_id AS chain_id,
        p.job_name AS job_name,
        p.target_kind AS target_kind,
        lower(p.target_address) AS target_address,
        p.snapshot_date AS snapshot_date,
        p.attempt_id AS attempt_id,
        p.publication_id AS publication_id,
        p.config_hash AS config_hash,
        p.anchor_block AS anchor_block,
        p.anchor_hash AS anchor_hash,
        p.universe_hash AS universe_hash,
        p.universe_size AS universe_size,
        p.result_digest AS result_digest,
        p.published_at AS published_at
    FROM {{ source('rpc_state_indexer', 'census_publications') }} AS p
    INNER JOIN attempts AS t
        ON p.chain_id = t.chain_id
       AND p.job_name = t.job_name
       AND p.target_kind = t.target_kind
       AND p.target_address = t.target_address
       AND p.snapshot_date = t.snapshot_date
       AND p.attempt_id = t.attempt_id
       AND p.integrity_mode = t.integrity_mode
       AND p.anchor_block = t.anchor_block
       AND p.anchor_hash = t.anchor_hash
       AND p.universe_hash = t.universe_hash
       AND p.universe_size = t.universe_size
       AND p.result_digest = t.result_digest
       AND p.batches_total = t.batches_total
       AND p.observations_total = t.observations_ok
    INNER JOIN anchors AS a
        ON p.chain_id = a.chain_id
       AND p.snapshot_date = a.snapshot_date
       AND p.anchor_block = a.anchor_block
       AND p.anchor_hash = a.anchor_hash
    WHERE p.attempt_id NOT IN (SELECT attempt_id FROM errored)
)

SELECT
    v.chain_id AS chain_id,
    v.job_name AS job_name,
    v.target_kind AS target_kind,
    v.target_address AS target_address,
    v.snapshot_date AS snapshot_date,
    argMax(v.attempt_id, tuple(v.published_at, toString(v.publication_id))) AS attempt_id,
    argMax(v.config_hash, tuple(v.published_at, toString(v.publication_id))) AS config_hash,
    argMax(v.anchor_block, tuple(v.published_at, toString(v.publication_id))) AS anchor_block,
    argMax(v.anchor_hash, tuple(v.published_at, toString(v.publication_id))) AS anchor_hash,
    argMax(v.universe_hash, tuple(v.published_at, toString(v.publication_id))) AS universe_hash,
    argMax(v.universe_size, tuple(v.published_at, toString(v.publication_id))) AS universe_size,
    argMax(v.result_digest, tuple(v.published_at, toString(v.publication_id))) AS result_digest,
    max(v.published_at) AS published_at
FROM verified AS v
GROUP BY v.chain_id, v.job_name, v.target_kind, v.target_address, v.snapshot_date
