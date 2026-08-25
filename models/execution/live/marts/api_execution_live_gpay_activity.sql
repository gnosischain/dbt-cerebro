{{
    config(
        materialized='view',
        tags=['live', 'execution', 'gpay', 'api']
    )
}}

-- Recent GP Payment / Fiat Top Up feed (last 30 min of cached live activity).
-- Anchored on HWM of int_live__gpay_activity_raw with a 60s reorg buffer.

WITH

hwm AS (
    SELECT max(block_timestamp) AS ts
    FROM {{ ref('int_live__gpay_activity_raw') }}
)

SELECT
    block_timestamp,
    block_number,
    transaction_hash,
    wallet_address,
    action,
    symbol,
    token_address,
    round(amount, 6)      AS amount,
    round(amount_usd, 2)  AS amount_usd
FROM {{ ref('int_live__gpay_activity_raw') }} FINAL
WHERE block_timestamp >= (SELECT ts FROM hwm) - INTERVAL 30 MINUTE
  AND block_timestamp <= (SELECT ts FROM hwm) - INTERVAL 60 SECOND
ORDER BY block_timestamp DESC
