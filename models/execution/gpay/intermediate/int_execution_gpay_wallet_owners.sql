{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    engine='ReplacingMergeTree()',
    order_by='(pay_wallet, owner)',
    partition_by='toStartOfMonth(block_timestamp)',
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
{{ apply_monthly_incremental_filter('co.became_owner_at', 'block_timestamp', false) }}
