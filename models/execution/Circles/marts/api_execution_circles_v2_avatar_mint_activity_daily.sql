{{
    config(
        materialized='view',
        tags=['production','execution','tier1','api:circles_v2_avatar_mint_activity_daily', 'granularity:daily']
    )
}}

-- Daily personal-mint activity per Circles v2 avatar.
--
-- A Circles v2 personal mint is a Hub TransferSingle event where
-- `from_address = 0x0000000000000000000000000000000000000000` and the
-- recipient is the avatar itself. Each row in
-- `int_execution_circles_v2_hub_transfers` with that shape represents
-- one personalMint() call (or the unscheduled mint that happens during
-- a transfer).
--
-- Output is one row per (avatar, date) with the number of mint events
-- and the total amount minted that day. Backs the "Mint Activity"
-- panel on the Circles Avatar tab.

SELECT
    to_address                                          AS avatar,
    toDate(block_timestamp)                             AS date,
    count()                                             AS mint_events,
    toFloat64(sum(amount_raw)) / pow(10, 18)            AS amount_minted
FROM {{ ref('int_execution_circles_v2_hub_transfers') }}
WHERE from_address = '0x0000000000000000000000000000000000000000'
  AND to_address  != '0x0000000000000000000000000000000000000000'
GROUP BY to_address, toDate(block_timestamp)
