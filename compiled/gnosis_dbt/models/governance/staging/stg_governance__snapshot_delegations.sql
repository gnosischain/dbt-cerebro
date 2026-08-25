

-- One row per (chain_id, tx_hash, log_index) DelegateRegistry event for the
-- gnosis.eth space. Source is rpc-log-indexer's reorg-safe projection (not a
-- ReplacingMergeTree ingest table), so no FINAL / ingested_at dedupe.
-- Action vocabulary is remapped to set/clear so downstream filters stay stable.
SELECT
    chain_id,
    tx_hash,
    block_number,
    log_index,
    toDateTime(block_timestamp) AS block_time,
    multiIf(
        action = 'SetDelegate', 'set',
        action = 'ClearDelegate', 'clear',
        action
    ) AS action,
    lower(delegator) AS delegator,
    lower(delegate)  AS delegate
FROM `rpc_log_indexer`.`v_delegate_events_gnosis`