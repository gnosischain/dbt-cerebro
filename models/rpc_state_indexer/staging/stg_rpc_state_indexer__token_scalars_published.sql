{{
  config(
    materialized='view',
    tags=['production','staging','rpc_state_indexer']
  )
}}

SELECT
    s.chain_id AS chain_id,
    s.job_name AS job_name,
    lower(s.token_address) AS token_address,
    s.snapshot_date AS snapshot_date,
    s.scalar_name AS scalar_name,
    s.scalar_raw AS scalar_raw
FROM (SELECT * FROM {{ source('rpc_state_indexer', 'token_scalars') }} FINAL) AS s
INNER JOIN {{ ref('stg_rpc_state_indexer__publications') }} AS p
    ON s.chain_id = p.chain_id
   AND s.job_name = p.job_name
   AND p.target_kind = 'token'
   AND lower(s.token_address) = p.target_address
   AND s.snapshot_date = p.snapshot_date
   AND s.attempt_id = p.attempt_id
