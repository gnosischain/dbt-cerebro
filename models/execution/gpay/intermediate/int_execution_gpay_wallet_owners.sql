{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    engine='ReplacingMergeTree()',
    order_by='(pay_wallet)',
    partition_by='toStartOfMonth(block_timestamp)',
    unique_key='(pay_wallet)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','execution','gpay']
  )
}}

{#
  Decode SafeSetup events from execution.logs for GPay wallets.
  SafeSetup(address indexed initiator, address[] owners, uint256 threshold, address initializer, address fallbackHandler)
  topic0 = keccak256("SafeSetup(address,address[],uint256,address,address)")

  ABI data layout (non-indexed params):
    Word 0: offset to owners[] → always 0x80 (128 bytes = 4 head slots)
    Word 1: threshold (uint256)
    Word 2: initializer (address)
    Word 3: fallbackHandler (address)
    Word 4: owners array length (N)
    Word 5+: owner addresses
#}

WITH gpay_wallets AS (
    SELECT address
    FROM {{ ref('stg_gpay__wallets') }}
),

safe_setup_logs AS (
    SELECT
        lower(replaceAll(address, '0x', ''))  AS addr_raw,
        block_timestamp,
        replaceAll(data, '0x', '')            AS data_hex
    FROM {{ source('execution', 'logs') }}
    WHERE lower(replaceAll(topic0, '0x', ''))
          = '141df868a6331af528e38c83b7aa03edc19be66e37ae67f9285bf4f8e3c6a1a8'
      AND lower(replaceAll(address, '0x', ''))
          IN (SELECT lower(replaceAll(address, '0x', '')) FROM gpay_wallets)
      AND block_timestamp >= toDateTime('2023-06-01')
      {% if is_incremental() %}
        AND block_timestamp > (SELECT coalesce(max(block_timestamp), toDateTime('1970-01-01')) FROM {{ this }})
      {% endif %}
)

SELECT
    concat('0x', addr_raw)                                                  AS pay_wallet,
    lower(concat('0x', substring(data_hex, 1 + 5*64 + 24, 40)))            AS owner,
    toUInt32(reinterpretAsUInt256(reverse(unhex(substring(data_hex, 1 + 1*64, 64))))) AS threshold,
    block_timestamp
FROM safe_setup_logs
