{{
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='delegator',
    tags=['production','governance']
  )
}}

-- Current delegation state per delegator: their latest event ordered by
-- (block_number, log_index) -- NOT block_time, which only has second
-- granularity and reliably ties within the same block/tx (e.g. a delegator
-- clearing and immediately re-setting in one transaction). A latest action
-- of 'clear' means no active delegation, so it has no row here -- this
-- model only ever holds currently-active delegator -> delegate edges.
WITH latest AS (
    SELECT
        delegator,
        delegate,
        action,
        block_time,
        tx_hash,
        row_number() OVER (
            PARTITION BY delegator
            ORDER BY block_number DESC, log_index DESC
        ) AS rn
    FROM {{ ref('stg_governance__snapshot_delegations') }}
)
SELECT
    delegator,
    delegate,
    block_time AS delegated_at,
    tx_hash
FROM latest
WHERE rn = 1 AND action = 'set'
