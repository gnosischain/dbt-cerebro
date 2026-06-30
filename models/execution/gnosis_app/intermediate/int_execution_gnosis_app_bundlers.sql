{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(address)',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','gnosis_app','bundlers'],
    pre_hook=["SET max_bytes_before_external_group_by = 3000000000", "SET join_algorithm = 'grace_hash'"],
    post_hook=["SET max_bytes_before_external_group_by = 0", "SET join_algorithm = 'default'"]
  )
}}

-- Dynamic Cometh ERC-4337 bundler allowlist for Gnosis App attribution.
-- Replaces the hand-maintained seeds/gnosis_app_relayers.csv, which went stale as
-- Cometh rotated bundlers: only 3 of ~61 real bundlers were listed, so ~10% of GA
-- users were missed (miss-rate jumped to 16-22% for Mar-2026+ cohorts when the new
-- 0x4337... fleet launched). Schema mirrors gnosis_app_relayers (address 0x-prefixed
-- lowercase, is_active) so consumers swap only the ref().
--
-- A bundler is included if it sends UserOps to the ERC-4337 EntryPoint AND either:
--   (a) matches the Cometh vanity prefix 0x4337..., OR
--   (b) relayed >= 1 tx carrying a Gnosis-App-specific, relayer-agnostic anchor:
--         - a CRC fee transfer (ERC-1155 Hub TransferSingle/Batch OR gCRC ERC-20
--           Transfer) to the GA fee receiver, OR
--         - a GA InvitationModule RegisterHuman (in-app invitation-at-scale onboarding),
-- plus the curated seed (belt-and-suspenders). Anchors do not depend on the relayer
-- set, so the allowlist self-maintains as the fleet rotates.

{% set entrypoint     = '0000000071727de22e5e9d8baf0edac6f37da032' %}
{% set fee_receiver   = '97fd8f7829a019946329f6d2e763a72741047518' %}
{% set erc20_transfer = 'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' %}
{% set launch         = '2025-11-12' %}

WITH anchor_txs AS (
    -- ERC-1155 CRC fee to the GA receiver
    SELECT replaceAll(lower(transaction_hash), '0x', '') AS txh
    FROM {{ ref('contracts_circles_v2_Hub_events') }}
    WHERE event_name IN ('TransferSingle','TransferBatch')
      AND lower(decoded_params['to']) = '0x{{ fee_receiver }}'
    UNION DISTINCT
    -- gCRC ERC-20 fee to the GA receiver (fees migrated to ERC-20 in June 2026)
    SELECT replaceAll(lower(transaction_hash), '0x', '')
    FROM {{ source('execution','logs') }}
    WHERE replaceAll(lower(topic0), '0x', '') = '{{ erc20_transfer }}'
      AND endsWith(replaceAll(lower(topic2), '0x', ''), '{{ fee_receiver }}')
      AND block_timestamp >= toDateTime('{{ launch }}')
    UNION DISTINCT
    -- GA InvitationModule (invitation-at-scale) RegisterHuman
    SELECT replaceAll(lower(transaction_hash), '0x', '')
    FROM {{ ref('contracts_circles_v2_InvitationModule_events') }}
),

entrypoint_txs AS (
    SELECT
        from_address                                   AS bundler,
        replaceAll(lower(transaction_hash), '0x', '')  AS txh
    FROM {{ source('execution','transactions') }}
    WHERE to_address = '{{ entrypoint }}'
      AND block_timestamp >= toDateTime('{{ launch }}')
),

classified AS (
    SELECT bundler, 'vanity_4337' AS source FROM entrypoint_txs WHERE startsWith(bundler, '4337')
    UNION ALL
    SELECT bundler, 'anchor'      AS source FROM entrypoint_txs WHERE txh IN (SELECT txh FROM anchor_txs)
    UNION ALL
    SELECT lower(replaceAll(address, '0x', '')) AS bundler, 'seed' AS source
    FROM {{ ref('gnosis_app_relayers') }}
    WHERE is_active = 1
)

SELECT
    concat('0x', bundler)                                          AS address,
    toUInt8(1)                                                     AS is_active,
    arrayStringConcat(arraySort(groupArray(DISTINCT source)), '+') AS source
FROM classified
WHERE bundler != '' AND bundler IS NOT NULL
GROUP BY bundler
