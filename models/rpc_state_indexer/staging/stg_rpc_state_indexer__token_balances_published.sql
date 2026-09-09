{{
  config(
    materialized='view',
    tags=['production','staging','rpc_state_indexer']
  )
}}

SELECT
    b.chain_id AS chain_id,
    b.job_name AS job_name,
    lower(b.token_address) AS token_address,
    b.snapshot_date AS snapshot_date,
    lower(b.holder_address) AS holder_address,
    b.balance_raw AS balance_raw
FROM (SELECT * FROM {{ source('rpc_state_indexer', 'token_balances') }} FINAL) AS b
INNER JOIN {{ ref('stg_rpc_state_indexer__publications') }} AS p
    ON b.chain_id = p.chain_id
   AND b.job_name = p.job_name
   AND p.target_kind = 'token'
   AND lower(b.token_address) = p.target_address
   AND b.snapshot_date = p.snapshot_date
   AND b.attempt_id = p.attempt_id
