{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(rate_feed_id, block_timestamp, log_index)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','celo','prices','oracle','mento']
  )
}}

-- Tracked Mento SortedOracles rate feeds on Celo, raw-sliced from
-- celo_execution.logs. MedianUpdated(address indexed token, uint256 value):
-- token (topic1) is the rateFeedId, value (data word 0) is the median rate
-- at Mento's 24-decimal fixidity.
--
-- Currently tracks a single feed: CELO/XAUt (rateFeedId 0xb1C7...b009),
-- wired by Celo governance CGP-0240 / Mento MGP-17 (May 2026) to price
-- XAUt0 as a gas currency. A ChainlinkRelayerV1 derives it as
-- Chainlink CELO/USD x inverse(Redstone XAUt/USDT) and heartbeats it to
-- SortedOracles once a day at 00:00 UTC (live since 2026-06-09). There is
-- NO direct Chainlink XAU aggregator on Celo (verified on-chain: no
-- AnswerUpdated emitter reports in the gold price range), so this relayed
-- rate is the only native on-chain gold price surface.
--
-- Raw slicing, not the decode macro: single event with a fixed layout on a
-- single well-known contract, same rationale as int_celo_gpay_roles_modules.
-- The feed list is inline (not a seed) while it has one entry; lift it into
-- a seed if/when more Mento feeds become relevant.
--
-- Full rebuild while the celo_execution backfill is in flight (rows for old
-- months keep appearing), same rationale as the other Celo native models.
-- Volume is one row per feed per day — trivially cheap forever.

SELECT
    rate_feed_id,
    feed_label,
    block_timestamp,
    block_number,
    log_index,
    rate
FROM (
    SELECT
        concat('0x', substring(lower(replaceAll(topic1, '0x', '')), 25, 40)) AS rate_feed_id,
        'CELO/XAUt'                                                          AS feed_label,
        block_timestamp,
        block_number,
        log_index,
        toFloat64(reinterpretAsUInt256(reverse(unhex(
            substring(replaceAll(data, '0x', ''), 1, 64)
        )))) / 1e24                                                          AS rate,
        row_number() OVER (
            PARTITION BY block_number, transaction_index, log_index
            ORDER BY insert_version DESC
        ) AS _dedup_rn
    FROM {{ source('celo_execution', 'logs') }}
    WHERE lower(replaceAll(address, '0x', '')) = 'efb84935239dacdecf7c5ba76d8de40b077b7b33'  -- SortedOracles
      AND replaceAll(topic0, '0x', '') = 'a9981ebfc3b766a742486e898f54959b050a66006dbce1a4155c1f84a08bcf41'  -- MedianUpdated
      AND lower(replaceAll(topic1, '0x', '')) = '000000000000000000000000b1c735ffd1b8f01316382e72bcc17d19493eb009'  -- CELO/XAUt
      AND block_timestamp >= toDateTime('2026-06-01')
)
WHERE _dedup_rn = 1
  AND rate > 0
