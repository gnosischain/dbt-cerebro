{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    engine='ReplacingMergeTree()',
    order_by='(block_timestamp, transaction_hash, log_index, batch_index)',
    partition_by='toStartOfMonth(block_timestamp)',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','circles_v2','mint_events'],
    pre_hook=["SET join_algorithm = 'grace_hash'"],
    post_hook=["SET join_algorithm = 'default'"]
  )
}}

-- One row per protocol mint, tagged with `mint_kind`.
--
-- Sourced from the DECODED Hub events in contracts_circles_v2_Hub_events
-- (NOT inferred from `from = 0x00…00` TransferSingle legs):
--
--   * personal  - PersonalMint event. The authoritative "human claimed their
--                 issuance" signal. to_address / token_address = the minting
--                 human; amount_raw = the event `amount`.
--                 (Inferring personal mints from every from=0x0 TransferSingle
--                 to a Human token over-counted ~4.5k avatars whose CRC was
--                 created only by inline auto-issuance legs that emit no
--                 PersonalMint event. This now matches Dune query_6317871 /
--                 hub_evt_personalmint: ~18.4k distinct humans.)
--   * group     - GroupMint event. to_address = receiver, token_address = group,
--                 operator = sender, amount_raw = Σ of the event `amounts[]`.
--   * migration - V1→V2 migration mints have NO Hub mint event, so this branch
--                 stays transfer-derived: from=0x0 TransferSingle whose operator
--                 is the Circles V2 Migration contract
--                 (contracts_circles_registry.contract_type = 'Migration').
--
-- token_id is not carried by the events and is NULL for personal/group rows
-- (populated only for migration rows sourced from the transfer leg). The
-- output schema is unchanged so downstream consumers stay source-compatible.

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

WITH migration_operators AS (
    SELECT DISTINCT lower(address) AS address
    FROM {{ ref('contracts_circles_registry') }}
    WHERE contract_type = 'Migration'
),

-- personal mints = authoritative PersonalMint event
personal AS (
    SELECT
        block_number,
        block_timestamp,
        transaction_hash,
        transaction_index,
        log_index,
        toUInt32(0)                                       AS batch_index,
        CAST(NULL AS Nullable(String))                    AS operator,
        lower(decoded_params['human'])                    AS to_address,
        CAST(NULL AS Nullable(String))                    AS token_id,
        lower(decoded_params['human'])                    AS token_address,
        toUInt256OrZero(coalesce(decoded_params['amount'], '0')) AS amount_raw,
        'CrcV2_PersonalMint'                              AS transfer_type,
        'personal'                                        AS mint_kind
    FROM {{ ref('contracts_circles_v2_Hub_events') }}
    WHERE event_name = 'PersonalMint'
      {% if start_month and end_month %}
        AND toStartOfMonth(block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('block_timestamp', 'block_timestamp', add_and=True) }}
      {% endif %}
),

-- group mints = GroupMint event (sum the amounts[] array)
grp AS (
    SELECT
        block_number,
        block_timestamp,
        transaction_hash,
        transaction_index,
        log_index,
        toUInt32(0)                                       AS batch_index,
        lower(decoded_params['sender'])                   AS operator,
        lower(decoded_params['receiver'])                 AS to_address,
        CAST(NULL AS Nullable(String))                    AS token_id,
        lower(decoded_params['group'])                    AS token_address,
        toUInt256(arraySum(arrayMap(
            x -> toUInt256OrZero(x),
            JSONExtract(coalesce(decoded_params['amounts'], '[]'), 'Array(String)')
        )))                                               AS amount_raw,
        'CrcV2_GroupMint'                                 AS transfer_type,
        'group'                                           AS mint_kind
    FROM {{ ref('contracts_circles_v2_Hub_events') }}
    WHERE event_name = 'GroupMint'
      {% if start_month and end_month %}
        AND toStartOfMonth(block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('block_timestamp', 'block_timestamp', add_and=True) }}
      {% endif %}
),

-- migration mints = no Hub event; from=0x0 legs by the Migration contract
migration AS (
    SELECT
        t.block_number,
        t.block_timestamp,
        t.transaction_hash,
        t.transaction_index,
        t.log_index,
        t.batch_index,
        t.operator,
        t.to_address,
        t.token_id,
        t.token_address,
        t.amount_raw,
        t.transfer_type,
        'migration'                                       AS mint_kind
    FROM {{ ref('int_execution_circles_v2_hub_transfers') }} t
    INNER JOIN migration_operators mo ON mo.address = lower(t.operator)
    WHERE t.from_address = '0x0000000000000000000000000000000000000000'
      AND t.to_address  != '0x0000000000000000000000000000000000000000'
      {% if start_month and end_month %}
        AND toStartOfMonth(t.block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(t.block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('t.block_timestamp', 'block_timestamp', add_and=True) }}
      {% endif %}
)

SELECT * FROM personal
UNION ALL
SELECT * FROM grp
UNION ALL
SELECT * FROM migration
