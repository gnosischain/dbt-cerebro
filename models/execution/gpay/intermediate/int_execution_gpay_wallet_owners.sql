{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    engine='ReplacingMergeTree()',
    order_by='(pay_wallet, owner)',
    partition_by='toStartOfMonth(block_timestamp)',
    unique_key='(pay_wallet, owner)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','execution','gpay']
  )
}}

{#
  Current owner snapshot for Gnosis Pay Safes.

  Thin filter over int_execution_safes_current_owners (the generic Safe
  ownership model) so the GPay fact tables automatically pick up post-setup
  owner changes (AddedOwner / RemovedOwner / ChangedThreshold) — not just
  the creation-time SafeSetup snapshot the previous version of this model
  was limited to.

  Output schema is preserved (pay_wallet, owner, threshold, block_timestamp)
  so downstream fact models do not need changes, but two semantic things
  shifted with this refactor:

    1. order_by / unique_key are now (pay_wallet, owner). The previous
       (pay_wallet) ordering caused ReplacingMergeTree to silently keep
       only one owner per multi-sig Safe on merge. Multi-owner Safes are
       now preserved correctly — row count for those Safes will increase.

    2. block_timestamp now means "last became-owner event time", not
       "Safe creation time". For an owner added post-setup, this is the
       AddedOwner event timestamp. For an owner who was removed and then
       re-added, this is the re-add timestamp.
#}

WITH gpay_safes AS (
    SELECT lower(address) AS pay_wallet
    FROM {{ ref('stg_gpay__wallets') }}
)

SELECT
    co.safe_address       AS pay_wallet,
    co.owner              AS owner,
    co.current_threshold  AS threshold,
    co.became_owner_at    AS block_timestamp
FROM {{ ref('int_execution_safes_current_owners') }} co
INNER JOIN gpay_safes gs
    ON co.safe_address = gs.pay_wallet
{% if is_incremental() %}
WHERE co.became_owner_at > (
    SELECT coalesce(max(block_timestamp), toDateTime('1970-01-01')) FROM {{ this }}
)
{% endif %}
