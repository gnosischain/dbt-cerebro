{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='safe_address',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','execution','celo','gpay','wallets']
  )
}}

{% set settlement = var('celo_gp_settlement_address') %}

-- Canonical Celo GP card Safe list, native-only. Issuance comes from the
-- append-only int_celo_gpay_wallet_events log (action='issued_at'); each
-- issued_at row is an immutable SafeSetup fact, written once, never revised.
--
-- first_spend_at / is_activated are derived directly from
-- int_celo_gpay_transfers_native (a transfer to the settlement bridge — the
-- same activation signal the Dune spine used), NOT via int_celo_gpay_activity:
-- that model depends on THIS one for wallet-membership classification, so
-- reading it here would create a dbt cycle. transfers_native does not depend on
-- wallets, so reading it directly is cycle-free. The full-table GROUP BY on
-- every rebuild is acceptable at scale by precedent — Gnosis Chain's own
-- int_execution_gpay_wallets does an equivalent full scan of the much larger
-- execution.logs on every run, materialized='table', successfully.
--
-- Deliberately materialized='table' (full rebuild), bounded by card count not
-- transaction volume, so it stays small and re-checks is_activated/first_spend_at
-- for every Safe on every run (a Safe issued today whose first spend happens
-- months from now is picked up correctly, since nothing here is windowed by age).

WITH issued AS (
    SELECT
        safe_address,
        min(event_time)          AS issued_at_ts,
        any(action_value)        AS owner_address,
        groupArray(action_value) AS owners
    FROM {{ ref('int_celo_gpay_wallet_events') }}
    WHERE action = 'issued_at'
    GROUP BY safe_address
),

activation AS (
    SELECT
        sender          AS safe_address,
        min(block_date) AS first_spend_at
    FROM {{ ref('int_celo_gpay_transfers_native') }}
    FINAL
    WHERE receiver = '{{ settlement }}'
    GROUP BY sender
)

SELECT
    i.safe_address,
    i.owner_address,
    i.owners,
    toDate(i.issued_at_ts)                        AS issued_at,
    -- block_date is a non-nullable Date, so an unmatched LEFT JOIN fills
    -- a.first_spend_at with ClickHouse's type default (1970-01-01), not
    -- NULL — nullIf converts that sentinel back into a real NULL. Without
    -- this, every unactivated wallet reads as "activated on day zero".
    nullIf(a.first_spend_at, toDate(0))           AS first_spend_at,
    nullIf(a.first_spend_at, toDate(0)) IS NOT NULL AS is_activated
FROM issued i
LEFT JOIN activation a ON a.safe_address = i.safe_address
