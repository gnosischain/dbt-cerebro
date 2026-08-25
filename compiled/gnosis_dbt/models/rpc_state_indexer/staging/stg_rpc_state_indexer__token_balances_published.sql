

SELECT
    chain_id,
    job_name,
    lower(token_address) AS token_address,
    snapshot_date,
    lower(holder_address) AS holder_address,
    balance_raw
FROM `rpc_state_indexer`.`v_token_balances_published`