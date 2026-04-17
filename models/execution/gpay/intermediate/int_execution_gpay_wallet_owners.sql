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

WITH gpay_safes AS (
    SELECT lower(address) AS pay_wallet
    FROM {{ ref('int_execution_gpay_wallets') }}
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
