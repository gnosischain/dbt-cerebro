

SELECT
    chain_id,
    job_name,
    lower(token_address) AS token_address,
    snapshot_date,
    scalar_name,
    scalar_raw
FROM `rpc_state_indexer`.`v_token_scalars_published`