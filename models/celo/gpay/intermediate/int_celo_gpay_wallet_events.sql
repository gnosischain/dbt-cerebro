{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(safe_address, action, action_value, event_time)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','celo','gpay','wallet_events']
  )
}}

-- Unified, append-only wallet-lifecycle event log (issued_at / add_owner /
-- remove_owner) for GP card Safes on Celo, derived NATIVELY from
-- celo_execution via int_celo_gpay_safe_events_native, gated to the GP
-- registry (int_celo_gpay_safe_registry). issued_at = the SafeSetup owner(s)
-- of a registered Safe — SafeSetup alone would capture every Safe on Celo, so
-- registry membership is the GP filter; event time is the SafeSetup block,
-- matching the Dune spine's issuance-timestamp semantics.
--
-- Full table rebuild (bounded by card count, not transaction volume). Consumed
-- by int_celo_gpay_wallets (issued_at only) and int_celo_gpay_safe_current_owners
-- (the full fold). This replaces the old crawlers_data.celo_gpay_wallet_events
-- (Dune) source: the Celo GP pipeline is now native-only on celo_execution
-- (the Dune/click-runner path was an MVP and is retired).

WITH registry AS (
    SELECT address FROM {{ ref('int_celo_gpay_safe_registry') }}
)

SELECT
    e.safe_address,
    CASE e.event_kind
        WHEN 'safe_setup'    THEN 'issued_at'
        WHEN 'added_owner'   THEN 'add_owner'
        WHEN 'removed_owner' THEN 'remove_owner'
    END                 AS action,
    e.owner             AS action_value,
    e.block_timestamp   AS event_time
FROM {{ ref('int_celo_gpay_safe_events_native') }} e
WHERE e.safe_address IN (SELECT address FROM registry)
