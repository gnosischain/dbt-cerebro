{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    engine='ReplacingMergeTree()',
    order_by='(date, gp_safe)',
    partition_by='toStartOfMonth(date)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','execution','gpay']
  )
}}

WITH gpay_safes AS (
    SELECT lower(address) AS pay_wallet FROM {{ ref('int_execution_gpay_wallets') }}
),

-- Spend.account is a per-card module, not the Safe itself; the bridge
-- resolves it to the Safe the module was enabled on.
account_safes AS (
    SELECT account, safe_address
    FROM {{ ref('int_execution_gpay_spender_accounts') }}
    WHERE safe_address IS NOT NULL
),

events_filtered AS (
    SELECT
        toDate(s.block_timestamp) AS date,
        lower(s.spend_account)    AS spend_account,
        s.spend_asset,
        s.spend_receiver
    FROM {{ ref('int_execution_gpay_spender_events') }} s
    WHERE s.event_name = 'Spend'
      AND s.spend_account IS NOT NULL
      AND toDate(s.block_timestamp) < today()
      {{ apply_monthly_incremental_filter('s.block_timestamp', 'date', add_and=True) }}
)

SELECT
    e.date,
    a.safe_address               AS gp_safe,
    count()                      AS spend_count,
    uniqExact(e.spend_asset)     AS distinct_assets,
    uniqExact(e.spend_receiver)  AS distinct_receivers
FROM events_filtered e
INNER JOIN account_safes a ON a.account = e.spend_account
INNER JOIN gpay_safes gs ON gs.pay_wallet = a.safe_address
GROUP BY e.date, a.safe_address
