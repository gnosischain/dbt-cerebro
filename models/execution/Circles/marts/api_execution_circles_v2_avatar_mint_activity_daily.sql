{{
    config(
        materialized='view',
        tags=['production', 'execution', 'tier1', 'api:circles_v2_avatar_mint_activity', 'granularity:daily']
    )
}}

-- Daily personal-mint activity per Circles v2 avatar.
--
-- A Circles v2 personal mint is a Hub TransferSingle event where
-- `from_address = 0x00…00` AND the minted token belongs to the recipient
-- (i.e. the avatar mints its own Circles to itself), and the avatar is a
-- Human. That excludes group mints (group CRC minted into a depositor's
-- wallet) and V1→V2 migrations, both of which previously contaminated
-- this view. See int_execution_circles_v2_mint_events for the classifier.
--
-- Output is one row per (avatar, date) with the number of mint events
-- and the total amount minted that day. Backs the "Mint Activity"
-- panel on the Circles Avatar tab.

SELECT
    to_address                                          AS avatar,
    toDate(block_timestamp)                             AS date,
    count()                                             AS mint_events,
    toFloat64(sum(amount_raw)) / pow(10, 18)            AS amount_minted
FROM {{ ref('int_execution_circles_v2_mint_events') }}
WHERE mint_kind = 'personal'
GROUP BY to_address, toDate(block_timestamp)
