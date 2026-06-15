{{
    config(
        materialized='view',
        tags=['production', 'execution', 'tier1', 'api:circles_v2_avatar_mint_activity', 'granularity:daily']
    )
}}

-- Daily personal-mint activity per Circles v2 avatar.
--
-- A Circles v2 personal mint = a decoded Hub `PersonalMint` event (a human
-- claiming their issuance). Sourced from int_execution_circles_v2_mint_events
-- filtered to mint_kind = 'personal', which now reads the PersonalMint event
-- directly — no longer inferred from `from = 0x00…00` TransferSingle legs,
-- which over-counted avatars via inline auto-issuance mints that emit no
-- PersonalMint event. Group mints and V1→V2 migrations are excluded. Matches
-- Dune query_6317871 / hub_evt_personalmint (~18.4k distinct humans).
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
