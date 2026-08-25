

-- Current delegation state per (delegator, chain_id): their latest event
-- ordered by (block_number, log_index) -- NOT block_time, which only has
-- second granularity and reliably ties within the same block/tx (e.g. a
-- delegator clearing and immediately re-setting in one transaction). A
-- latest action of 'clear' means no active delegation, so it has no row
-- here -- this model only ever holds currently-active edges.
-- Grain is per chain because Snapshot reads both registries; the same
-- address can (and does) delegate on chain 1 and chain 100 independently.
WITH latest AS (
    SELECT
        chain_id,
        delegator,
        delegate,
        action,
        block_time,
        tx_hash,
        row_number() OVER (
            PARTITION BY chain_id, delegator
            ORDER BY block_number DESC, log_index DESC
        ) AS rn
    FROM `dbt`.`stg_governance__snapshot_delegations`
)
SELECT
    chain_id,
    delegator,
    delegate,
    block_time AS delegated_at,
    tx_hash
FROM latest
WHERE rn = 1 AND action = 'set'